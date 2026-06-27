# Trilogy failure analysis — 20260627-155703

- Run `20260627-155703` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 230 | failed: 30 (13%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 22 | 73% |
| `syntax-parse` | 7 | 23% |
| `syntax-missing-alias` | 1 | 3% |

## Detail

### `other`

- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'next_year.week_seq': matches ['next_year.weekly_dow.sales.date.week_seq', 'next_year.weekly_dow.week_seq']. Qualify the full path to disambiguate.
  ```
- `trilogy run query02.preql`

  ```text
  Resolution error in query02.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 27). The requested concepts split into 8 disconnected subgraphs: {curr.ws, nxt.tot}; {local._virt_filter_tot_3658236264874198}; {local._virt_filter_tot_4662246062701109}; {local._virt_filter_tot_4994720592717996}; {local._virt_filter_tot_7113132754175075}; {local._virt_filter_tot_7838499380833314}; {local._virt_filter_tot_7907278579064643}; {local._virt_filter_tot_8654975212883761}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Ambiguous reference 'ratios.ws': matches ['ratios.curr.ws', 'ratios.nxt.ws']. Qualify the full path to disambiguate.
  ```
- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 43 column 12 (char 1845). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(coalesce(ref:sales_data._sch,ref:returns_data._rch))` (line 25), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(coalesce(ref:sales_data._sch,ref:returns_data._rch)) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(coalesce(ref:sales_data._sch,ref:returns_data._rch))` (line 25), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(coalesce(ref:sales_data._sch,ref:returns_data._rch)) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: (_duckdb.BinderException) Binder Error: GROUPING child "COALESCE('CATALOG', sparkling.returns_data__rch)" must be a grouping column

  LINE 219:     grouping(coalesce( 'CATALOG' ,"sparkling"."returns_data__rch...
                ^
  [SQL:
  WITH
  scrawny as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "s_channel_dim_id",
      "s_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "s_channel_dim_text_id"
  FROM
      "catalog_page" as "s_catalog_dim_unified"
  WHERE
      "s_catalog_dim_unified"."CP_CATALOG_PAGE_ID" is not null

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_dim_unified"."S_STORE_SK" as "s_channel_dim_id",
      "s_store_dim_unified"."S_STORE_ID" as "s_channel_dim_text_id"
  FROM
      "store" as "s_store_dim_unified"
  WHERE
      "s_store_dim_unified"."S_STORE_ID" is not null

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_dim_unified"."web_site_sk" as "s_channel_dim_id",
      "s_web_dim_unified"."web_site_id" as "s_channel_dim_text_id"
  FROM
      "web_site" as "s_web_dim_unified"
  WHERE
      "s_web_dim_unified"."web_site_id" is not null
  ),
  divergent as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "s_channel_dim_id",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_id",
      "s_catalog_sales_unified"."CS_NET_PROFIT" as "s_net_profit"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_STORE_SK" as "s_channel_dim_id",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_id",
      "s_store_sales_unified"."SS_NET_PROFIT" as "s_net_profit"
  FROM
      "store_sales" as "s_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_WEB_SITE_SK" as "s_channel_dim_id",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_id",
      "s_web_sales_unified"."WS_NET_PROFIT" as "s_net_profit"
  FROM
      "web_sales" as "s_web_sales_unified"),
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_dim_return_unified"."CP_CATALOG_PAGE_SK" as "s_return_channel_dim_id",
      "s_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "s_return_channel_dim_text_id"
  FROM
      "catalog_page" as "s_catalog_dim_return_unified"
  WHERE
      "s_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" is not null

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_dim_return_unified"."S_STORE_SK" as "s_return_channel_dim_id",
      "s_store_dim_return_unified"."S_STORE_ID" as "s_return_channel_dim_text_id"
  FROM
      "store" as "s_store_dim_return_unified"
  WHERE
      "s_store_dim_return_unified"."S_STORE_ID" is not null

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_dim_return_unified"."web_site_sk" as "s_return_channel_dim_id",
      "s_web_dim_return_unified"."web_site_id" as "s_return_channel_dim_text_id"
  FROM
      "web_site" as "s_web_dim_return_unified"
  WHERE
      "s_web_dim_return_unified"."web_site_id" is not null
  ),
  abundant as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_RETURN_AMOUNT" as "s_return_amount",
      "s_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_catalog_returns_unified"."CR_NET_LOSS" as "s_return_net_loss"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_RETURN_AMT" as "s_return_amount",
      "s_store_returns_unified"."SR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_store_returns_unified"."SR_NET_LOSS" as "s_return_net_loss"
  FROM
      "store_returns" as "s_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_returns_unified"."WR_ITEM_SK" as "s_item_id",
      "s_web_returns_unified"."WR_ORDER_NUMBER" as "s_order_id",
      "s_web_returns_unified"."WR_RETURN_AMT" as "s_return_amount",
      "s_web_returns_unified"."WR_RETURNED_DATE_SK" as "s_return_date_id",
      "s_web_returns_unified"."WR_NET_LOSS" as "s_return_net_loss"
  FROM
      "web_returns" as "s_web_returns_unified"),
  yummy as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "s_return_channel_dim_id"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_STORE_SK" as "s_return_channel_dim_id"
  FROM
      "store_returns" as "s_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_WEB_SITE_SK" as "s_return_channel_dim_id"
  FROM
      "web_sales" as "s_web_sales_unified"),
  charming as (
  SELECT
      "divergent"."s_channel" as "_sales_data__sch",
      "scrawny"."s_channel_dim_text_id" as "_sales_data__eid",
      sum(coalesce("divergent"."s_net_profit",0)) as "_sales_data__gross_sales"
  FROM
      "divergent"
      INNER JOIN "scrawny" on "divergent"."s_channel" = "scrawny"."s_channel" AND "divergent"."s_channel_dim_id" = "scrawny"."s_channel_dim_id"
      INNER JOIN "date_dim" as "s_date_date" on "divergent"."s_date_id" = "s_date_date"."D_DATE_SK"
  WHERE
      cast("s_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "scrawny"."s_channel_dim_text_id" is not null

  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "cheerful"."s_return_channel_dim_text_id" as "_returns_data__reid",
      coalesce("abundant"."s_channel","cheerful"."s_channel","yummy"."s_channel") as "_returns_data__rch",
      sum(coalesce("abundant"."s_return_amount",0)) as "_returns_data__total_returns",
      sum(coalesce("abundant"."s_return_net_loss",0)) as "_returns_data__return_loss"
  FROM
      "yummy"
      INNER JOIN "abundant" on "yummy"."s_channel" = "abundant"."s_channel" AND "yummy"."s_item_id" = "abundant"."s_item_id" AND "yummy"."s_order_id" = "abundant"."s_order_id"
      INNER JOIN "date_dim" as "s_return_date_date" on "abundant"."s_return_date_id" = "s_return_date_date"."D_DATE_SK"
      INNER JOIN "cheerful" on "yummy"."s_return_channel_dim_id" = "cheerful"."s_return_channel_dim_id" AND coalesce("yummy"."s_channel", "abundant"."s_channel") = "cheerful"."s_channel"
  WHERE
      cast("s_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and "cheerful"."s_return_channel_dim_text_id" is not null

  GROUP BY
      1,
      2),
  premium as (
  SELECT
      "charming"."_sales_data__eid" as "sales_data__eid",
      "charming"."_sales_data__gross_sales" as "sales_data__gross_sales",
      "charming"."_sales_data__sch" as "sales_data__sch"
  FROM
      "charming"),
  young as (
  SELECT
      "vacuous"."_returns_data__rch" as "returns_data__rch",
      "vacuous"."_returns_data__reid" as "returns_data__reid",
      "vacuous"."_returns_data__return_loss" as "returns_data__return_loss",
      "vacuous"."_returns_data__total_returns" as "returns_data__total_returns"
  FROM
      "vacuous"),
  puzzled as (
  SELECT
      "premium"."sales_data__gross_sales" as "sales_data__gross_sales",
      "young"."returns_data__return_loss" as "returns_data__return_loss",
      "young"."returns_data__total_returns" as "returns_data__total_returns",
      coalesce("premium"."sales_data__eid","young"."returns_data__reid") as "returns_data__reid",
      coalesce("premium"."sales_data__eid","young"."returns_data__reid") as "sales_data__eid",
      coalesce("premium"."sales_data__sch","young"."returns_data__rch") as "returns_data__rch",
      coalesce("premium"."sales_data__sch","young"."returns_data__rch") as "sales_data__sch",
      coalesce(coalesce("premium"."sales_data__eid","young"."returns_data__reid"),coalesce("premium"."sales_data__eid","young"."returns_data__reid")) as "_virt_func_coalesce_8286327182651781",
      coalesce(coalesce("premium"."sales_data__sch","young"."returns_data__rch"),coalesce("premium"."sales_data__sch","young"."returns_data__rch")) as "_virt_func_coalesce_9650987905243674"
  FROM
      "premium"
      FULL JOIN "young" on "premium"."sales_data__eid" = "young"."returns_data__reid" AND "premium"."sales_data__sch" = "young"."returns_data__rch"),
  sparkling as (
  SELECT
      "young"."returns_data__rch" as "returns_data__rch",
      "young"."returns_data__reid" as "returns_data__reid",
      coalesce( 'CATALOG' ,"young"."returns_data__rch") as "_virt_func_coalesce_9650987905243674",
      coalesce("young"."returns_data__reid","young"."returns_data__reid") as "_virt_func_coalesce_8286327182651781"
  FROM
      "young"
  GROUP BY
      1,
      2,
      3,
      4),
  abhorrent as (
  SELECT
      "sparkling"."_virt_func_coalesce_8286327182651781" as "_virt_func_coalesce_8286327182651781",
      "sparkling"."_virt_func_coalesce_9650987905243674" as "_virt_func_coalesce_9650987905243674",
      grouping(coalesce( 'CATALOG' ,"sparkling"."returns_data__rch")) as "_g1",
      grouping(coalesce( 'CATALOG' ,"sparkling"."returns_data__rch")) as "_virt_agg_grouping_1635810502380519",
      grouping(coalesce("sparkling"."returns_data__reid","sparkling"."returns_data__reid")) as "_virt_agg_grouping_4828181370496500"
  FROM
      "sparkling"
  GROUP BY
      ROLLUP (2, 1))
  SELECT
      CASE
  	WHEN "abhorrent"."_virt_agg_grouping_1635810502380519" = 1 THEN 'grand total'
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'STORE' THEN 'store channel'
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'CATALOG' THEN 'catalog channel'
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'WEB' THEN 'web channel'
  	END as "channel_name",
      CASE
  	WHEN "abhorrent"."_virt_agg_grouping_4828181370496500" = 1 THEN null
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'STORE' THEN ('store' || coalesce("puzzled"."sales_data__eid","puzzled"."returns_data__reid"))
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'CATALOG' THEN ('catalog_page' || coalesce("puzzled"."sales_data__eid","puzzled"."returns_data__reid"))
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'WEB' THEN ('web_site' || coalesce("puzzled"."sales_data__eid","puzzled"."returns_data__reid"))
  	END as "entity_id",
      coalesce("puzzled"."sales_data__gross_sales",0) as "total_gross_sales",
      coalesce("puzzled"."returns_data__total_returns",0) as "total_returns",
      coalesce("puzzled"."sales_data__gross_sales",0) - coalesce("puzzled"."returns_data__return_loss",0) as "net_profit"
  FROM
      "puzzled"
      FULL JOIN "abhorrent" on "puzzled"."_virt_func_coalesce_8286327182651781" is not distinct from "abhorrent"."_virt_func_coalesce_8286327182651781" AND "puzzled"."_virt_func_coalesce_9650987905243674" is not distinct from "abhorrent"."_virt_func_coalesce_9650987905243674"
  ORDER BY
      "abhorrent"."_g1" asc nulls first,
      CASE
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'STORE' THEN 1
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'CATALOG' THEN 2
  	WHEN coalesce("puzzled"."sales_data__sch","puzzled"."returns_data__rch") = 'WEB' THEN 3
  	ELSE 0
  	END asc nulls first,
      coalesce("puzzled"."sales_data__eid","puzzled"."returns_data__reid") asc nulls first
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg_sale', which is not in the SELECT projection (line 9). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg_sale
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run --import raw.physical_returns:sr select sr.ticket_number, sr.return_amount, sr.return_quantity, limit 1;`

  ```text
  Syntax error in stdin: Undefined concept: sr.return_quantity (line 2, col 44, in SELECT). Suggestions: ['sr.return_amount', 'sr.return_date.id', 'sr.return_date.year']
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'local.max_customer_total', which is not in the SELECT projection (line 25). To make them available, you may add it to  the SELECT. Prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.max_customer_total
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query54.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: store_sales.customer_demographics.marital_status (line 27, col 9, in WHERE). Suggestions: ['store_sales.customer.demographics.marital_status', 'store_sales.return_customer.demographics.marital_status', 'store_sales.customer_demographic.marital_status', 'catalog_returns.sales.ship_customer.demographics.marital_status', 'catalog_returns.sales.bill_customer.demographics.marital_status', 'catalog_returns.sales.bill_customer_demographic.marital_status']
  ```
- `trilogy run --import raw/all_sales:s select s.date.year as yr, s.item.brand_id as brand, s.item.class_id as cls, s.item.category_id as cat, s.item.manufactur…io where s.item.category='Books' and yr in (2001,2002) having prev_qty is not null and ratio < 0.9 order by qty - prev_qty, amt - prev_amt limit 100;`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Ambiguous reference 'yearly_with_lag.year': matches ['yearly_with_lag.yearly.deduped.year', 'yearly_with_lag.yearly.year']. Qualify the full path to disambiguate.
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Ambiguous reference 'yearly_lag.yr': matches ['yearly_lag.yearly.deduped.yr', 'yearly_lag.yearly.yr']. Qualify the full path to disambiguate.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql --content import raw.all_sales as sales;

# Current year's weekly day-of-week sales (WEB + CATALOG)
rowset curr as
where sal… = 6) / max(nxt.tot ? nxt.dow = 6), 2) as saturday,
inner join curr.ws + 53 = nxt.ws
inner join curr.dow = nxt.dow
order by curr.ws asc nulls first
;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | rowset curr as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...of-week sales (WEB + CATALOG) ??? rowset curr as where sales.cha...

  Write stats: received 1509 chars / 1509 bytes; tail: …'r join curr.dow = nxt.dow\\norder by curr.ws asc nulls first\\n;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as s;

# Overall average sale value (quantity * list_price) across all three channels in 1999…e
order by
    s.channel nulls first,
    s.item.brand_id nulls first,
    s.item.class_id nulls first,
    s.item.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:1
     |
  21 | having
     | ^---
     |
     = expected limit or order_by
  Location:
  ...class_id, s.item.category_id) ??? having     -- For the per-chan...

  Write stats: received 1092 chars / 1092 bytes; tail: …'d nulls first,\\n    s.item.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss where ss.date.year = 2001 and ss.return_date.year in (2001, 2002) select ss.item.text_id, count(ss.ticket_number) as cnt group by ss.item.text_id order by cnt desc limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ount(ss.ticket_number) as cnt ??? group by ss.item.text_id order...
  ```
- `trilogy file write query23.preql -e -c import raw.store_sales as store;
import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.item as …les
order by
    combined_sales.last_name asc nulls first,
    combined_sales.first_name asc nulls first,
    total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 38:5
     |
  38 |     where catalog.sold_date.year = 2000
     |     ^---
     |
     = expected tvf_rel_arg
  Location:
   union(     # Catalog arm     ??? where catalog.sold_date.year =...

  Write stats: received 2563 chars / 2563 bytes; tail: …'asc nulls first,\\n    total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.store_sales as store;
import raw.date…ment_times_50
where store.customer.id in eligible_customer_ids
order by segment nulls first, customer_count nulls first, segment_times_50
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:39
     |
  23 | auto eligible_customer_ids <- distinct(union(catalog_customers, web_customers));
     |                                       ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...gible_customer_ids <- distinct ??? (union(catalog_customers, web_...

  Write stats: received 1661 chars / 1661 bytes; tail: …'st, customer_count nulls first, segment_times_50\\nlimit 100;\\n'.
  ```
- `trilogy file write query54.preql --content import raw.catalog_sales as catalog;
import raw.web_sales as web;
import raw.store_sales as store;

# month_seq of…store.customer.id in cat_cust or store.customer.id in web_cust
order by segment nulls first, customer_count nulls first, segment_times_50
limit 100;
`

  ```text
  refused to write 'query54.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:46
     |
  29 | auto segment <- round(cust_total_price / 50) by store.customer.id;
     |                                              ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   round(cust_total_price / 50) ??? by store.customer.id;  # Count...

  Write stats: received 1487 chars / 1487 bytes; tail: …'st, customer_count nulls first, segment_times_50\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw/all_sales:s select s.item.brand_id, s.item.class_id, s.item.category_id, s.item.manufacturer_id, s.date.year, sum(coalesce(s.quantit…0)) as net_amt where s.item.category='Books' and s.date.year in (2001,2002) group by brand_id, class_id, category_id, manufacturer_id, year limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nd s.date.year in (2001,2002) ??? group by brand_id, class_id, c...
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.catalog_sales:catalog --import raw.web_sales:web --import raw.store_sales:store 
auto dec1998_month_seq <- 1187;
auto cat_cust <- ca…date.month_seq between 1188 and 1190
  and store.store.county = store.customer.address.county
  and store.store.state = store.customer.address.state;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count_distinct(store.customer.id) as id_count_distinct`
  Location:
  ...t_distinct(store.customer.id) ??? where (store.customer.id in ca...
  ```
