# Trilogy failure analysis — 20260625-191717

- Run `20260625-191717` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 280 | failed: 44 (16%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 24 | 55% |
| `syntax-parse` | 14 | 32% |
| `undefined-concept` | 2 | 5% |
| `syntax-missing-alias` | 1 | 2% |
| `type-error` | 1 | 2% |
| `cli-misuse` | 1 | 2% |
| `join-resolution` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: 3 undefined concept references; fix all before re-running:
    - weekly_daily.week_seq (line 20, col 5, in SELECT); did you mean: weekly_daily.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_daily.week_seq (line 18, col 7, in WHERE); did you mean: weekly_daily.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
    - weekly_daily.week_seq (line 28, col 10, in ORDER BY); did you mean: weekly_daily.sales.date.week_seq, sales.date.week_seq, sales.return_date.week_seq, sales.billing_customer.first_sales_date.week_seq, sales.billing_customer.first_shipto_date.week_seq, sales.ship_customer.first_sales_date.week_seq?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "sales_channel_dim_text_id"
  FROM
      "catalog_page" as "sales_catalog_dim_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_dim_unified"."S_STORE_SK" as "sales_channel_dim_id",
      "sales_store_dim_unified"."S_STORE_ID" as "sales_channel_dim_text_id"
  FROM
      "store" as "sales_store_dim_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_dim_unified"."web_site_sk" as "sales_channel_dim_id",
      "sales_web_dim_unified"."web_site_id" as "sales_channel_dim_text_id"
  FROM
      "web_site" as "sales_web_dim_unified"),
  abundant as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
      "sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_catalog_returns_unified"."CR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
      "sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_store_returns_unified"."SR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
      "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
      "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
      "sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "sales_return_date_id",
      "sales_web_returns_unified"."WR_NET_LOSS" as "sales_return_net_loss"
  FROM
      "web_returns" as "sales_web_returns_unified"),
  yummy as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
      "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
      "sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "sales_return_channel_dim_id"
  FROM
      "catalog_returns" as "sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
      "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
      "sales_store_returns_unified"."SR_STORE_SK" as "sales_return_channel_dim_id"
  FROM
      "store_returns" as "sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
      "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_return_channel_dim_id"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  concerned as (
  SELECT
       'CATALOG'  as "sales_channel",
      "sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "sales_channel_dim_id",
      "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
      "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
      "sales_catalog_sales_unified"."CS_NET_PROFIT" as "sales_net_profit",
      "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "catalog_sales" as "sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "sales_channel",
      "sales_store_sales_unified"."SS_STORE_SK" as "sales_channel_dim_id",
      "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
      "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
      "sales_store_sales_unified"."SS_NET_PROFIT" as "sales_net_profit",
      "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id"
  FROM
      "store_sales" as "sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "sales_channel",
      "sales_web_sales_unified"."WS_WEB_SITE_SK" as "sales_channel_dim_id",
      "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
      "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
      "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
      "sales_web_sales_unified"."WS_NET_PROFIT" as "sales_net_profit",
      "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id"
  FROM
      "web_sales" as "sales_web_sales_unified"),
  abhorrent as (
  SELECT
      "cheerful"."sales_channel_dim_text_id" as "sales_channel_dim_text_id",
      CASE WHEN cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "concerned"."sales_channel_dim_id" is not null THEN "concerned"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_5125991069469977",
      CASE WHEN cast("sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "concerned"."sales_channel_dim_id" is not null THEN "concerned"."sales_net_profit" ELSE NULL END as "_virt_filter_net_profit_8840377095342353",
      CASE WHEN cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "yummy"."sales_return_channel_dim_id" is not null THEN "abundant"."sales_return_amount" ELSE NULL END as "_virt_filter_return_amount_8598493087517663",
      CASE WHEN cast("sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "yummy"."sales_return_channel_dim_id" is not null THEN "abundant"."sales_return_net_loss" ELSE NULL END as "_virt_filter_return_net_loss_9515339227637990",
      coalesce("abundant"."sales_channel","cheerful"."sales_channel","concerned"."sales_channel","yummy"."sales_channel") as "sales_channel"
  FROM
      "concerned"
      LEFT OUTER JOIN "date_dim" as "sales_date_date" on "concerned"."sales_date_id" = "sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "cheerful" on "concerned"."sales_channel" = "cheerful"."sales_channel" AND "concerned"."sales_channel_dim_id" = "cheerful"."sales_channel_dim_id"
      LEFT OUTER JOIN "yummy" on "concerned"."sales_channel" = "yummy"."sales_channel" AND "concerned"."sales_item_id" = "yummy"."sales_item_id" AND "concerned"."sales_order_id" = "yummy"."sales_order_id"
      FULL JOIN "abundant" on "yummy"."sales_item_id" = "abundant"."sales_item_id" AND "yummy"."sales_order_id" = "abundant"."sales_order_id" AND coalesce("yummy"."sales_channel", "cheerful"."sales_channel") = "abundant"."sales_channel"
      FULL JOIN "date_dim" as "sales_return_date_date" on "abundant"."sales_return_date_id" = "sales_return_date_date"."D_DATE_SK"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      coalesce("abundant"."sales_item_id","concerned"."sales_item_id","yummy"."sales_item_id"),
      coalesce("abundant"."sales_order_id","concerned"."sales_order_id","yummy"."sales_order_id")),
  scrawny as (
  SELECT
      CASE
  	WHEN "abhorrent"."sales_channel" is null THEN null
  	WHEN "abhorrent"."sales_channel" = 'STORE' THEN 'store channel'
  	WHEN "abhorrent"."sales_channel" = 'CATALOG' THEN 'catalog channel'
  	WHEN "abhorrent"."sales_channel" = 'WEB' THEN 'web channel'
  	END as "channel_type",
      CASE
  	WHEN "abhorrent"."sales_channel_dim_text_id" is null THEN null
  	WHEN "abhorrent"."sales_channel" = 'STORE' THEN ('store' || "abhorrent"."sales_channel_dim_text_id")
  	WHEN "abhorrent"."sales_channel" = 'CATALOG' THEN ('catalog_page' || "abhorrent"."sales_channel_dim_text_id")
  	WHEN "abhorrent"."sales_channel" = 'WEB' THEN ('web_site' || "abhorrent"."sales_channel_dim_text_id")
  	END as "entity_identifier",
      sum("abhorrent"."_virt_filter_ext_sales_price_5125991069469977") as "raw_gross_sales",
      sum("abhorrent"."_virt_filter_net_profit_8840377095342353") as "raw_profit",
      sum("abhorrent"."_virt_filter_return_amount_8598493087517663") as "raw_returns",
      sum("abhorrent"."_virt_filter_return_net_loss_9515339227637990") as "raw_loss"
  FROM
      "abhorrent"
  GROUP BY
      ROLLUP ("abhorrent"."sales_channel", "abhorrent"."sales_channel_dim_text_id")
  HAVING
      ( ( coalesce("raw_gross_sales",0) + coalesce("raw_returns",0) ) + coalesce("raw_profit",0) ) + coalesce("raw_loss",0) != 0
  )
  SELECT
      "scrawny"."channel_type" as "channel_type",
      "scrawny"."entity_identifier" as "entity_identifier",
      "scrawny"."raw_gross_sales" as "raw_gross_sales",
      "scrawny"."raw_returns" as "raw_returns",
      "scrawny"."raw_profit" as "raw_profit",
      "scrawny"."raw_loss" as "raw_loss"
  FROM
      "scrawny"
  ORDER BY
      CASE
  	WHEN INVALID_REFERENCE_BUG is null THEN 1
  	ELSE 0
  	END asc,
      INVALID_REFERENCE_BUG asc,
      CASE
  	WHEN INVALID_REFERENCE_BUG is null THEN 1
  	ELSE 0
  	END asc,
      INVALID_REFERENCE_BUG asc
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query05.preql`

  ```text
  Unable to transform python value of type '<enum 'MagicConstants'>' to DuckDB LogicalType\n[SQL: \nWITH \nscrawny as (\nSELECT\n     'CATALOG'  as \"sales_channel\",\n    \"sales_catalog_dim_unified\".\"CP_CATALOG_PAGE_SK\" as \"sales_channel_dim_id\",\n    \"sales_catalog_dim_unified\".\"CP_CATALOG_PAGE_ID\" as \"sales_channel_dim_text_id\"\nFROM\n    \"catalog_page\" as \"sales_catalog_dim_unified\"\nUNION ALL\nSELECT\n     'STORE'  as \"sales_channel\",\n    \"sales_store_dim_unified\".\"S_STORE_SK\" as \"sales_channel_dim_id\",\n    \"sales_store_dim_unified\".\"S_STORE_ID\" as \"sales_cha
  …
  t_rows_eid\" is null THEN 1\n\tELSE 0\n\tEND asc,\n    \"slow\".\"_output_rows_eid\" asc\nLIMIT (100)]\n[parameters: (<MagicConstants.NULL: 'null'>, <MagicConstants.NULL: 'null'>, <MagicConstants.NULL: 'null'>)]\n(Background on this error at: https://sqlalche.me/e/20/tw8g)",
    "error_type": "NotSupportedError"
  }
  {
    "event": "summary",
    "statements": 1,
    "duration_ms": 394.476,
    "ok": false,
    "rows": 0
  }
  {
    "event": "output_truncated",
    "dropped_events": 1,
    "note": "Output exceeded the tool cap; trailing events dropped. Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: combined.yr. Suggestions: ['combined.store_rev_by_yr.yr', 'store_rev_by_yr.yr', 'web_rev_by_yr.yr', 'combined.web_rev', 'combined.store_rev', '_combined_web_rev']
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: combined.yr. Suggestions: ['combined.s_rev.yr', 's_rev.yr', 'w_rev.yr', 'combined.web_rev', 'combined.store_rev']
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Have {'RowsetNode<s02.cust_code,s02.fname,s02.lname...3 more>': None} and need s01.rev > 0 and w01.rev > 0 and case(WHEN w01.rev > 0 THEN divide(parenthetical(subtract(w02.rev@Grain<w02.cid>,w01.rev@Grain<w01.cid>)),w01.rev@Grain<w01.cid>),BuildCaseElse(expr=cast(0,Trait<NUMERIC, ['usd']>))) > case(WHEN s01.rev > 0 THEN divide(parenthetical(subtract(s02.rev@Grain<s02.cid,s02.cust_code>,s01.rev@Grain<s01.cid>)),s01.rev@Grain<s01.cid>),BuildCaseElse(expr=cast(0,Trait<NUMERIC, ['usd']>)))
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Have {'RowsetNode<s02.cust_code,s02.fname,s02.lname...3 more>': None} and need s01.rev > 0 and w01.rev > 0 and case(WHEN w01.rev > 0 THEN divide(parenthetical(subtract(w02.rev@Grain<w02.cid>,w01.rev@Grain<w01.cid>)),w01.rev@Grain<w01.cid>),BuildCaseElse(expr=cast(0,Trait<NUMERIC, ['usd']>))) > case(WHEN s01.rev > 0 THEN divide(parenthetical(subtract(s02.rev@Grain<s02.cid,s02.cust_code>,s01.rev@Grain<s01.cid>)),s01.rev@Grain<s01.cid>),BuildCaseElse(expr=cast(0,Trait<NUMERIC, ['usd']>)))
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: all_data.cid. Suggestions: ['all_data.s01.cid', 's01.cid', 's02.cid', 'w01.cid', 'w02.cid', 'all_data.ccode']
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Have {'RowsetNode<s02.cust_code,s02.fname,s02.lname...3 more>': None} and need s01.rev > 0 and w01.rev > 0 and divide(parenthetical(subtract(w02.rev@Grain<w02.cid>,w01.rev@Grain<w01.cid>)),w01.rev@Grain<w01.cid>) > divide(parenthetical(subtract(s02.rev@Grain<s02.cid,s02.cust_code>,s01.rev@Grain<s01.cid>)),s01.rev@Grain<s01.cid>)
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 43 column 3 (char 1571). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Tuple must have same type for all elements
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 19). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query17.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 8 column 14 (char 412). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.max_cust_total', which is not in the SELECT projection (line 33). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.max_cust_total
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query54.preql`

  ```text
  Unexpected error in query54.preql: Multiple where clauses are not supported
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'cat_sales_item.cum_ext_list_price', 'cat_returns_item.cum_refund', which are not in the SELECT projection (line 20). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cat_sales_item.cum_ext_list_price, --cat_returns_item.cum_refund
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: qualifying_items.item_text_id. Suggestions: ['qualifying_items.cat_sales_item.item_text_id', 'cat_sales_item.item_text_id', 'cat_returns_item.item_text_id']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: cat_sales_item.cat_sales_item.item_text_id. Suggestions: ['cat_sales_item.item_text_id', 'cat_returns_item.item_text_id', '_cat_sales_item_item_text_id']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.item.id, count(1) as cnt where all_sales.item.category = 'Books' and all_sales.order_id = 10302;`

  ```text
  Syntax error in stdin: Have {'MergeNode<all_sales.item.category,all_sales.item.id,all_sales.order_id>': None} and need all_sales.item.category = Books and all_sales.order_id = 10302
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 8 undefined concept references; fix all before re-running:
    - agg_data.yr (line 31, in SELECT); did you mean: agg_data.line_items.yr, line_items.yr, agg_data.mid, agg_data.cid, agg_data.bid?
    - agg_data.bid (line 34, col 5, in SELECT); did you mean: agg_data.line_items.bid, line_items.bid, agg_data.mid, agg_data.cid, agg_data.cat_id?
    - agg_data.cid (line 35, col 5, in SELECT); did you mean: agg_data.line_items.cid, line_items.cid, agg_data.mid, agg_data.bid, agg_data.cat_id?
    - agg_data.cat_id (line 36, col 5, in SELECT); did you mean: agg_data.line_items.cat_id, line_items.cat_id, agg_data.cid, agg_data.mid, agg_data.bid?
    - agg_data.mid (line 37, col 5, in SELECT); did you mean: agg_data.line_items.mid, line_items.mid, agg_data.cid, agg_data.bid, agg_data.cat_id?
    - agg_data.yr (line 38, col 118, in SELECT); did you mean: agg_data.line_items.yr, line_items.yr, agg_data.mid, agg_data.cid, agg_data.bid?
    - agg_data.yr (line 40, col 140, in SELECT); did you mean: agg_data.line_items.yr, line_items.yr, agg_data.mid, agg_data.cid, agg_data.bid?
    - agg_data.yr (line 41, col 140, in SELECT); did you mean: agg_data.line_items.yr, line_items.yr, agg_data.mid, agg_data.cid, agg_data.bid?
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:sales select sales.channel as ch, sales.channel_dim_text_id as sid, count(*) as cnt where sales.channel_dim_id is not null and sales.date.date between '2000-08-23' and '2000-09-06' group by 1,2 limit 5;`

  ```text
  Syntax error in stdin:  --> 2:69
    |
  2 | select sales.channel as ch, sales.channel_dim_text_id as sid, count(*) as cnt where sales.channel_dim_id is not null and sales.date.date between '2000-08-23' and '2000-09-06' group by 1,2 limit 5;
    |                                                                     ^---
    |
    = expected sum_operator
  Location:
  ...nel_dim_text_id as sid, count( ??? *) as cnt where sales.channel_...
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Channel label helper
with sales_arm as
  where sales.date.date between '2000-08-…n combined.channel is null then 1 else 0 end,
  combined.channel,
  case when combined.entity is null then 1 else 0 end,
  combined.entity
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup combined.channel, co...

  Write stats: received 2844 chars / 2844 bytes; tail: …'tity is null then 1 else 0 end,\\n  combined.entity\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:sales select sales.channel as ch, sales.channel_dim_text_id as eid, coalesce(sum(sales.ext_sales_price ? sales.date.date b…nd sales.channel_dim_id is not null), 0) by rollup sales.channel, sales.channel_dim_text_id as total where sales.channel_dim_id is not null limit 10;`

  ```text
  Syntax error in stdin: Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...annel_dim_id is not null), 0) ??? by rollup sales.channel, sales...
  ```
- `trilogy run --import raw/all_sales:sales select sales.channel_dim_text_id as sid, sales.return_channel_dim_text_id as rid, sales.is_returned, sum(sales.return_amount) as ra where sales.channel_dim_id is null and sales.return_channel_dim_id is not null group by 1,2,3 limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...rn_channel_dim_id is not null ??? group by 1,2,3 limit 5;
  ```
- `trilogy file write query05.preql --content import raw.all_sales as sales;

# Aggregate sales by sale entity
select
  case
    when sales.channel is null then… when channel_type is null then 1 else 0 end,
  channel_type,
  case when entity_identifier is null then 1 else 0 end,
  entity_identifier
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...l_dim_id is not null), 0)     ??? by rollup sales.channel, sales...

  Write stats: received 1697 chars / 1697 bytes; tail: …'er is null then 1 else 0 end,\\n  entity_identifier\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.item…  combined.channel nulls first,
    combined.brand_id nulls first,
    combined.class_id nulls first,
    combined.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:29
    |
  7 | auto store_combo <- distinct(store.item.brand_id, store.item.class_id, store.item.category_id) ? year(store.date.date) between 1999 and 2001;
    |                             ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...1 auto store_combo <- distinct ??? (store.item.brand_id, store.it...

  Write stats: received 3935 chars / 3935 bytes; tail: …'nulls first,\\n    combined.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;
import raw.item as item;

# Combos that appear in all 3 channels 1999-2001
# Count …ales.channel nulls first,
    sales.item.brand_id nulls first,
    sales.item.class_id nulls first,
    sales.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   having     channel_count = 3 ??? select     combos_in_channels....

  Write stats: received 1545 chars / 1545 bytes; tail: …'lls first,\\n    sales.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.date.quarter_name, count(store_sales.line_item) as cnt where store_sales.date.year = 2001 group by store_sales.date.quarter_name order by store_sales.date.quarter_name;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   store_sales.date.year = 2001 ??? group by store_sales.date.quar...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as item;…mbined.total) as total_sales
order by combined.last_name asc nulls first, combined.first_name asc nulls first, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:20
     |
  11 |   (count(ss.item.id, ss.date.id) by substring(ss.item.desc, 1, 30), ss.item.id > 4
     |                    ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...item.id ?    (count(ss.item.id ??? , ss.date.id) by substring(ss....

  Write stats: received 2073 chars / 2073 bytes; tail: …'name asc nulls first, total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as item;…t_name,
    sum(combined.total) as total_sales
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | rowset freq_items as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...t to define the frequent set. ??? rowset freq_items as where ss....

  Write stats: received 2353 chars / 2359 bytes; tail: …'name asc nulls first, total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query54.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as web_sale…l_price / 50) * 50 as segment_times_50
order by
    segment asc nulls first,
    customer_count asc nulls first,
    segment_times_50 asc
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:3
     |
  10 |   union
     |   ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ....sold_date.month_seq = 1187   ??? union   web_sales.billing_cust...

  Write stats: received 1340 chars / 1340 bytes; tail: …'_count asc nulls first,\\n    segment_times_50 asc\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…_item.cum_ext_list_price,
    cat_returns_item.cum_refund
having
    cat_sales_item.cum_ext_list_price > 2 * cat_returns_item.cum_refund
;

select 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:9
     |
  29 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ns_item.cum_refund ;  select 1 ??? ;

  Write stats: received 869 chars / 869 bytes; tail: …'xt_list_price > 2 * cat_returns_item.cum_refund\\n;\\n\\nselect 1;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

# Per-item cumula…_item.cum_ext_list_price,
    cat_returns_item.cum_refund
having
    cat_sales_item.cum_ext_list_price > 2 * cat_returns_item.cum_refund
;

select 1;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:9
     |
  30 | select 1;
     |         ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ns_item.cum_refund ;  select 1 ??? ;

  Write stats: received 812 chars / 812 bytes; tail: …'xt_list_price > 2 * cat_returns_item.cum_refund\\n;\\n\\nselect 1;'.
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.order_id, all_sales.item.id, count(1) as cnt where all_sales.item.category = 'Books' and all_sales.order_id = 10302 group by all_sales.channel, all_sales.order_id, all_sales.item.id limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nd all_sales.order_id = 10302 ??? group by all_sales.channel, al...
  ```

### `undefined-concept`

- `trilogy explore query11.preql`

  ```text
  Failed to parse query11.preql: (UndefinedConceptException(...), "Undefined concept: combined.yr. Suggestions: ['combined.s_rev.yr', 's_rev.yr', 'w_rev.yr', 'combined.web_rev', 'combined.store_rev']")
  ```
- `trilogy explore query64.preql --regex qualifying_items`

  ```text
  Failed to parse query64.preql: (UndefinedConceptException(...), "Undefined concept: qualifying_items.item_text_id. Suggestions: ['qualifying_items.cat_sales_item.item_text_id', 'cat_sales_item.item_text_id', 'cat_returns_item.item_text_id']")
  ```

### `syntax-missing-alias`

- `trilogy run --import raw/all_sales:sales select distinct sales.channel, sales.channel_dim_text_id limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct sales.channel as distinct_sales_channel`
  Location:
  ...les as sales; select distinct ??? sales.channel, sales.channel_d...
  ```

### `type-error`

- `trilogy run --import raw/all_sales:sales select sales.channel as ch, sales.channel_dim_text_id as sid where sales.channel_dim_id is not null and sales.date.date between '2000-08-23' and '2000-09-06' limit 5;`

  ```text
  Syntax error in stdin: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `cli-misuse`

- `trilogy explore raw/customer_address.preql`

  ```text
  Invalid value for 'PATH': File 'raw/customer_address.preql' does not exist.
  ```

### `join-resolution`

- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.order_id, all_sales.item.id where all_sales.item.category = 'Books' and all_sales.order_id = 10302 limit 20;`

  ```text
  Resolution error in stdin: Query is unresolvable: no complete sources found for output concepts {'all_sales.order_id'}. These concepts could only be resolved from partial sources.
  ```
