# Trilogy failure analysis — 20260625-155234

- Run `20260625-155234` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 362 | failed: 66 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 32 | 48% |
| `syntax-parse` | 27 | 41% |
| `syntax-missing-alias` | 4 | 6% |
| `cli-misuse` | 2 | 3% |
| `type-error` | 1 | 2% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: weekly_dow.week_seq. Suggestions: ['sales.date.week_seq', 'sales.return_date.week_seq', 'sales.billing_customer.first_sales_date.week_seq', 'sales.billing_customer.first_shipto_date.week_seq', 'sales.ship_customer.first_sales_date.week_seq', 'sales.ship_customer.first_shipto_date.week_seq']
  ```
- `trilogy run query02.preql`

  ```text
  Syntax error in query02.preql: Undefined concept: ws_2001.week_seq (line 29, col 30, in WHERE). Suggestions: ['ws_2001.sales.date.week_seq', 'sales.date.week_seq', 'sales.return_date.week_seq', 'sales.billing_customer.first_sales_date.week_seq', 'sales.billing_customer.first_shipto_date.week_seq', 'sales.ship_customer.first_sales_date.week_seq']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: sales_agg.channel. Suggestions: ['sales_agg.all_sales.channel', 'all_sales.channel', 'returns_agg.all_sales.channel', 'all_sales.channel_dim_id']
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept local.channel_type with grain Grain<Abstract> and lineage case(WHEN grouping(ref:combined.channel)<abstract> = 0 and grouping(ref:combined.entity_id)<abstract> = 1 THEN concat(ref:combined.channel, channel),WHEN grouping(ref:combined.channel)<abstract> = 1 and grouping(ref:combined.entity_id)<abstract> = 1 THEN grand total,ELSE concat(ref:combined.channel, channel)). This is likely due to a circular reference.
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin: union(...) requires at least two relational arms.
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Unexpected error in stdin: Recursion error building concept local.channel with grain Grain<Abstract> and lineage UnionSelectLineage(selects=[SelectLineage(selection=[ref:local.___tvf_arm_0_channel, ref:local.___tvf_arm_0_entity_id, ref:local.___tvf_arm_0_gross_sales, ref:local.___tvf_arm_0_total_returns, ref:local.___tvf_arm_0_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_0_channel': local.___tvf_arm_0_channel@Grain<Abstract>, 'local.___tvf_arm_0_entity_id': local.___tvf_arm_0_entity_id@Grain<all_sales.channel_dim_id>, 'local.___tvf_arm_0_gross_sales': local.___tvf_arm_0_gross_sales@Grain<all_sales.channel,all_sales.channel_dim_text_id>, 'local.___tvf_arm_0_total_returns': local.___tvf_arm_0_total_returns@Grain<all_sales.channel,all_sales.channel_dim_text_id>|ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, 'local.___tvf_arm_0_net_profit': local.___tvf_arm_0_net_profit@Grain<all_sales.channel,all_sales.channel_dim_text_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'all_sales.channel', 'all_sales.channel_dim_text_id'}, where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, component_order=['all_sales.channel', 'all_sales.channel_dim_text_id']), where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:local.___tvf_arm_1_channel, ref:local.___tvf_arm_1_entity_id, ref:local.___tvf_arm_1_gross_sales, ref:local.___tvf_arm_1_total_returns, ref:local.___tvf_arm_1_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_1_channel': local.___tvf_arm_1_channel@Grain<Abstract>, 'local.___tvf_arm_1_entity_id': local.___tvf_arm_1_entity_id@Grain<all_sales.return_channel_dim_id>, 'local.___tvf_arm_1_gross_sales': local.___tvf_arm_1_gross_sales@Grain<all_sales.channel,all_sales.return_channel_dim_text_id>|ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, 'local.___tvf_arm_1_total_returns': local.___tvf_arm_1_total_returns@Grain<all_sales.channel,all_sales.return_channel_dim_text_id>, 'local.___tvf_arm_1_net_profit': local.___tvf_arm_1_net_profit@Grain<all_sales.channel,all_sales.item.id,all_sales.order_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'all_sales.channel', 'all_sales.return_channel_dim_text_id'}, where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, component_order=['all_sales.channel', 'all_sales.return_channel_dim_text_id']), where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[])], align=AlignClause(items=[AlignItem(alias='channel', concepts=[ref:local.___tvf_arm_0_channel, ref:local.___tvf_arm_1_channel], namespace='local', hidden=False), AlignItem(alias='entity_id', concepts=[ref:local.___tvf_arm_0_entity_id, ref:local.___tvf_arm_1_entity_id], namespace='local', hidden=False), AlignItem(alias='gross_sales', concepts=[ref:local.___tvf_arm_0_gross_sales, ref:local.___tvf_arm_1_gross_sales], namespace='local', hidden=False), AlignItem(alias='total_returns', concepts=[ref:local.___tvf_arm_0_total_returns, ref:local.___tvf_arm_1_total_returns], namespace='local', hidden=False), AlignItem(alias='net_profit', concepts=[ref:local.___tvf_arm_0_net_profit, ref:local.___tvf_arm_1_net_profit], namespace='local', hidden=False)]), namespace='local', hidden_components=set(), order_by=None, limit=None, where_clause=None, having_clause=None, derive=None). This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept local.channel_type with grain Grain<Abstract> and lineage case(WHEN grouping(ref:combined.channel)<abstract> = 0 and grouping(ref:combined.entity_id)<abstract> = 1 THEN concat(ref:combined.channel, channel),WHEN grouping(ref:combined.channel)<abstract> = 1 and grouping(ref:combined.entity_id)<abstract> = 1 THEN grand total,ELSE concat(ref:combined.channel, channel)). This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: 5 undefined concept references; fix all before re-running:
    - c.channel (line 39, col 5, in SELECT); did you mean: all_sales.channel, sales_data.channel, return_data.channel, combined.channel, _combined_channel?
    - c.entity_id (line 40, col 5, in SELECT); did you mean: combined.entity_id, _combined_entity_id?
    - c.gross_sales (line 41, col 5, in SELECT); did you mean: sales_data.gross_sales, combined.gross_sales, _combined_gross_sales?
    - c.total_returns (line 42, col 5, in SELECT); did you mean: return_data.total_returns, combined.total_returns, _combined_total_returns?
    - c.net_profit (line 43, col 5, in SELECT); did you mean: all_sales.net_profit, combined.net_profit, _combined_net_profit?
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept local.channel_type with grain Grain<Abstract> and lineage case(WHEN grouping(ref:combined.channel)<abstract> = 0 and grouping(ref:combined.entity_id)<abstract> = 0 THEN concat(ref:combined.channel, channel),WHEN grouping(ref:combined.channel)<abstract> = 0 and grouping(ref:combined.entity_id)<abstract> = 1 THEN concat(ref:combined.channel, channel),ELSE grand total). This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: (_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups

  LINE 230:     grouping("premium"."combined_chan") as "g_chan",
                ^
  [SQL:
  WITH
  late as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_unified"."S_STORE_SK" as "all_sales_channel_dim_id",
      "all_sales_store_dim_unified"."S_STORE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_unified"."web_site_sk" as "all_sales_channel_dim_id",
      "all_sales_web_dim_unified"."web_site_id" as "all_sales_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_unified"),
  friendly as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "all_sales_channel_dim_id",
      "all_sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_catalog_sales_unified"."CS_NET_PROFIT" as "all_sales_net_profit"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      "all_sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" is not null and cast("all_sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_STORE_SK" as "all_sales_channel_dim_id",
      "all_sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_store_sales_unified"."SS_NET_PROFIT" as "all_sales_net_profit"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_store_sales_unified"."SS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      "all_sales_store_sales_unified"."SS_STORE_SK" is not null and cast("all_sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" as "all_sales_channel_dim_id",
      "all_sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_web_sales_unified"."WS_NET_PROFIT" as "all_sales_net_profit"
  FROM
      "web_sales" as "all_sales_web_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_web_sales_unified"."WS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" is not null and cast("all_sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'
  ),
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_SK" as "all_sales_return_channel_dim_id",
      "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_return_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_return_unified"."S_STORE_SK" as "all_sales_return_channel_dim_id",
      "all_sales_store_dim_return_unified"."S_STORE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_return_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_return_unified"."web_site_sk" as "all_sales_return_channel_dim_id",
      "all_sales_web_dim_return_unified"."web_site_id" as "all_sales_return_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_return_unified"),
  abundant as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_returns_unified"."CR_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "all_sales_return_amount",
      "all_sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "all_sales_return_date_id",
      "all_sales_catalog_returns_unified"."CR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "catalog_returns" as "all_sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_returns_unified"."SR_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_returns_unified"."SR_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_returns_unified"."SR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "all_sales_return_date_id",
      "all_sales_store_returns_unified"."SR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "store_returns" as "all_sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_returns_unified"."WR_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_returns_unified"."WR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_returns_unified"."WR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "all_sales_return_date_id",
      "all_sales_web_returns_unified"."WR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "web_returns" as "all_sales_web_returns_unified"),
  yummy as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_returns_unified"."CR_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "all_sales_return_channel_dim_id"
  FROM
      "catalog_returns" as "all_sales_catalog_returns_unified"
  WHERE
      "all_sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" is not null

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_returns_unified"."SR_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_returns_unified"."SR_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_returns_unified"."SR_STORE_SK" as "all_sales_return_channel_dim_id"
  FROM
      "store_returns" as "all_sales_store_returns_unified"
  WHERE
      "all_sales_store_returns_unified"."SR_STORE_SK" is not null

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_sales_unified"."WS_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" as "all_sales_return_channel_dim_id"
  FROM
      "web_sales" as "all_sales_web_sales_unified"
  WHERE
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" is not null
  ),
  divergent as (
  SELECT
      "friendly"."all_sales_channel" as "_sales_data_schannel",
      "late"."all_sales_channel_dim_text_id" as "_sales_data_sale_entity_id",
      sum("friendly"."all_sales_ext_sales_price") as "_sales_data_gross_sales",
      sum("friendly"."all_sales_net_profit") as "_sales_data_sale_net_profit"
  FROM
      "friendly"
      LEFT OUTER JOIN "late" on "friendly"."all_sales_channel" = "late"."all_sales_channel" AND "friendly"."all_sales_channel_dim_id" = "late"."all_sales_channel_dim_id"
  WHERE
      "friendly"."all_sales_channel_dim_id" is not null

  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "cheerful"."all_sales_return_channel_dim_text_id" as "_return_data_return_entity_id",
      coalesce("abundant"."all_sales_channel","cheerful"."all_sales_channel","yummy"."all_sales_channel") as "_return_data_rchannel",
      sum("abundant"."all_sales_return_amount") as "_return_data_total_returns",
      sum("abundant"."all_sales_return_net_loss") as "_return_data_return_net_loss"
  FROM
      "yummy"
      INNER JOIN "abundant" on "yummy"."all_sales_channel" = "abundant"."all_sales_channel" AND "yummy"."all_sales_item_id" = "abundant"."all_sales_item_id" AND "yummy"."all_sales_order_id" = "abundant"."all_sales_order_id"
      INNER JOIN "date_dim" as "all_sales_return_date_date" on "abundant"."all_sales_return_date_id" = "all_sales_return_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "cheerful" on "yummy"."all_sales_return_channel_dim_id" = "cheerful"."all_sales_return_channel_dim_id" AND coalesce("yummy"."all_sales_channel", "abundant"."all_sales_channel") = "cheerful"."all_sales_channel"
  WHERE
      cast("all_sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and coalesce("cheerful"."all_sales_return_channel_dim_id","yummy"."all_sales_return_channel_dim_id") is not null

  GROUP BY
      1,
      2),
  charming as (
  SELECT
      "divergent"."_sales_data_gross_sales" as "sales_data_gross_sales",
      "divergent"."_sales_data_sale_entity_id" as "sales_data_sale_entity_id",
      "divergent"."_sales_data_sale_net_profit" as "sales_data_sale_net_profit",
      "divergent"."_sales_data_schannel" as "sales_data_schannel"
  FROM
      "divergent"),
  young as (
  SELECT
      "vacuous"."_return_data_rchannel" as "return_data_rchannel",
      "vacuous"."_return_data_return_entity_id" as "return_data_return_entity_id",
      "vacuous"."_return_data_return_net_loss" as "return_data_return_net_loss",
      "vacuous"."_return_data_total_returns" as "return_data_total_returns"
  FROM
      "vacuous"),
  protective as (
  SELECT
      coalesce("charming"."sales_data_gross_sales",0) as "_combined_gross_sales",
      coalesce("charming"."sales_data_sale_net_profit",0) - coalesce("young"."return_data_return_net_loss",0) as "_combined_net_profit",
      coalesce("young"."return_data_total_returns",0) as "_combined_total_returns",
      coalesce(coalesce("charming"."sales_data_sale_entity_id","young"."return_data_return_entity_id"),coalesce("charming"."sales_data_sale_entity_id","young"."return_data_return_entity_id")) as "_combined_eid",
      coalesce(coalesce("charming"."sales_data_schannel","young"."return_data_rchannel"),coalesce("charming"."sales_data_schannel","young"."return_data_rchannel")) as "_combined_chan"
  FROM
      "charming"
      FULL JOIN "young" on "charming"."sales_data_sale_entity_id" = "young"."return_data_return_entity_id" AND "charming"."sales_data_schannel" = "young"."return_data_rchannel"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  premium as (
  SELECT
      "protective"."_combined_chan" as "combined_chan",
      "protective"."_combined_eid" as "combined_eid",
      "protective"."_combined_gross_sales" as "combined_gross_sales",
      "protective"."_combined_net_profit" as "combined_net_profit",
      "protective"."_combined_total_returns" as "combined_total_returns"
  FROM
      "protective"),
  waggish as (
  SELECT
      "premium"."combined_chan" as "combined_chan",
      "premium"."combined_eid" as "combined_eid",
      sum("premium"."combined_gross_sales") as "chan_eid_gross_sales",
      sum("premium"."combined_net_profit") as "chan_eid_net_profit",
      sum("premium"."combined_total_returns") as "chan_eid_total_returns"
  FROM
      "premium"
  GROUP BY
      ROLLUP (1, 2)),
  puzzled as (
  SELECT
      grouping("premium"."combined_chan") as "g_chan",
      grouping("premium"."combined_eid") as "g_eid"
  FROM
      "premium")
  SELECT
      CASE
  	WHEN "puzzled"."g_chan" = 0 and "puzzled"."g_eid" = 1 THEN ("waggish"."combined_chan" || ' channel')
  	WHEN "puzzled"."g_chan" = 1 and "puzzled"."g_eid" = 1 THEN 'grand total'
  	ELSE ("waggish"."combined_chan" || ' channel')
  	END as "channel_type",
      CASE
  	WHEN "puzzled"."g_eid" = 1 THEN cast(null as string)
  	WHEN "waggish"."combined_chan" = 'STORE' THEN ('store' || "waggish"."combined_eid")
  	WHEN "waggish"."combined_chan" = 'CATALOG' THEN ('catalog_page' || "waggish"."combined_eid")
  	WHEN "waggish"."combined_chan" = 'WEB' THEN ('web_site' || "waggish"."combined_eid")
  	END as "entity_identifier",
      "waggish"."chan_eid_gross_sales" as "total_gross_sales",
      "waggish"."chan_eid_total_returns" as "total_returns_amount",
      "waggish"."chan_eid_net_profit" as "net_profit_amount"
  FROM
      "waggish"
      FULL JOIN "puzzled" on 1=1
  GROUP BY
      1,
      2,
      3,
      4,
      5
  ORDER BY
      CASE
  	"waggish"."combined_chan"
  	WHEN 'STORE' THEN 1
  	WHEN 'CATALOG' THEN 2
  	WHEN 'WEB' THEN 3
  	ELSE 4
  	END asc,
      "waggish"."combined_eid" asc
  LIMIT (100)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: (_duckdb.BinderException) Binder Error: GROUPING child "combined_chan" must be a grouping column

  LINE 243:     grouping("waggish"."combined_chan") as "g_chan",
                ^
  [SQL:
  WITH
  late as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_SK" as "all_sales_channel_dim_id",
      "all_sales_catalog_dim_unified"."CP_CATALOG_PAGE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_unified"."S_STORE_SK" as "all_sales_channel_dim_id",
      "all_sales_store_dim_unified"."S_STORE_ID" as "all_sales_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_unified"."web_site_sk" as "all_sales_channel_dim_id",
      "all_sales_web_dim_unified"."web_site_id" as "all_sales_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_unified"),
  friendly as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "all_sales_channel_dim_id",
      "all_sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_catalog_sales_unified"."CS_NET_PROFIT" as "all_sales_net_profit"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      "all_sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" is not null and cast("all_sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_STORE_SK" as "all_sales_channel_dim_id",
      "all_sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_store_sales_unified"."SS_NET_PROFIT" as "all_sales_net_profit"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_store_sales_unified"."SS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      "all_sales_store_sales_unified"."SS_STORE_SK" is not null and cast("all_sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" as "all_sales_channel_dim_id",
      "all_sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "all_sales_ext_sales_price",
      "all_sales_web_sales_unified"."WS_NET_PROFIT" as "all_sales_net_profit"
  FROM
      "web_sales" as "all_sales_web_sales_unified"
      INNER JOIN "date_dim" as "all_sales_date_date" on "all_sales_web_sales_unified"."WS_SOLD_DATE_SK" = "all_sales_date_date"."D_DATE_SK"
  WHERE
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" is not null and cast("all_sales_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06'
  ),
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_SK" as "all_sales_return_channel_dim_id",
      "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_return_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_return_unified"."S_STORE_SK" as "all_sales_return_channel_dim_id",
      "all_sales_store_dim_return_unified"."S_STORE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_return_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_return_unified"."web_site_sk" as "all_sales_return_channel_dim_id",
      "all_sales_web_dim_return_unified"."web_site_id" as "all_sales_return_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_return_unified"),
  abundant as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_returns_unified"."CR_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "all_sales_return_amount",
      "all_sales_catalog_returns_unified"."CR_RETURNED_DATE_SK" as "all_sales_return_date_id",
      "all_sales_catalog_returns_unified"."CR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "catalog_returns" as "all_sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_returns_unified"."SR_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_returns_unified"."SR_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_returns_unified"."SR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_store_returns_unified"."SR_RETURNED_DATE_SK" as "all_sales_return_date_id",
      "all_sales_store_returns_unified"."SR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "store_returns" as "all_sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_returns_unified"."WR_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_returns_unified"."WR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_returns_unified"."WR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_web_returns_unified"."WR_RETURNED_DATE_SK" as "all_sales_return_date_id",
      "all_sales_web_returns_unified"."WR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "web_returns" as "all_sales_web_returns_unified"),
  yummy as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_returns_unified"."CR_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" as "all_sales_return_channel_dim_id"
  FROM
      "catalog_returns" as "all_sales_catalog_returns_unified"
  WHERE
      "all_sales_catalog_returns_unified"."CR_CATALOG_PAGE_SK" is not null

  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_returns_unified"."SR_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_returns_unified"."SR_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_returns_unified"."SR_STORE_SK" as "all_sales_return_channel_dim_id"
  FROM
      "store_returns" as "all_sales_store_returns_unified"
  WHERE
      "all_sales_store_returns_unified"."SR_STORE_SK" is not null

  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_sales_unified"."WS_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" as "all_sales_return_channel_dim_id"
  FROM
      "web_sales" as "all_sales_web_sales_unified"
  WHERE
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" is not null
  ),
  divergent as (
  SELECT
      "friendly"."all_sales_channel" as "_sales_data_schannel",
      "late"."all_sales_channel_dim_text_id" as "_sales_data_sale_entity_id",
      sum("friendly"."all_sales_ext_sales_price") as "_sales_data_gross_sales",
      sum("friendly"."all_sales_net_profit") as "_sales_data_sale_net_profit"
  FROM
      "friendly"
      LEFT OUTER JOIN "late" on "friendly"."all_sales_channel" = "late"."all_sales_channel" AND "friendly"."all_sales_channel_dim_id" = "late"."all_sales_channel_dim_id"
  WHERE
      "friendly"."all_sales_channel_dim_id" is not null

  GROUP BY
      1,
      2),
  vacuous as (
  SELECT
      "cheerful"."all_sales_return_channel_dim_text_id" as "_return_data_return_entity_id",
      coalesce("abundant"."all_sales_channel","cheerful"."all_sales_channel","yummy"."all_sales_channel") as "_return_data_rchannel",
      sum("abundant"."all_sales_return_amount") as "_return_data_total_returns",
      sum("abundant"."all_sales_return_net_loss") as "_return_data_return_net_loss"
  FROM
      "yummy"
      INNER JOIN "abundant" on "yummy"."all_sales_channel" = "abundant"."all_sales_channel" AND "yummy"."all_sales_item_id" = "abundant"."all_sales_item_id" AND "yummy"."all_sales_order_id" = "abundant"."all_sales_order_id"
      INNER JOIN "date_dim" as "all_sales_return_date_date" on "abundant"."all_sales_return_date_id" = "all_sales_return_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "cheerful" on "yummy"."all_sales_return_channel_dim_id" = "cheerful"."all_sales_return_channel_dim_id" AND coalesce("yummy"."all_sales_channel", "abundant"."all_sales_channel") = "cheerful"."all_sales_channel"
  WHERE
      cast("all_sales_return_date_date"."D_DATE" as date) BETWEEN date '2000-08-23' AND date '2000-09-06' and coalesce("cheerful"."all_sales_return_channel_dim_id","yummy"."all_sales_return_channel_dim_id") is not null

  GROUP BY
      1,
      2),
  charming as (
  SELECT
      "divergent"."_sales_data_gross_sales" as "sales_data_gross_sales",
      "divergent"."_sales_data_sale_entity_id" as "sales_data_sale_entity_id",
      "divergent"."_sales_data_sale_net_profit" as "sales_data_sale_net_profit",
      "divergent"."_sales_data_schannel" as "sales_data_schannel"
  FROM
      "divergent"),
  young as (
  SELECT
      "vacuous"."_return_data_rchannel" as "return_data_rchannel",
      "vacuous"."_return_data_return_entity_id" as "return_data_return_entity_id",
      "vacuous"."_return_data_return_net_loss" as "return_data_return_net_loss",
      "vacuous"."_return_data_total_returns" as "return_data_total_returns"
  FROM
      "vacuous"),
  protective as (
  SELECT
      coalesce("charming"."sales_data_gross_sales",0) as "_combined_gross_sales",
      coalesce("charming"."sales_data_sale_net_profit",0) - coalesce("young"."return_data_return_net_loss",0) as "_combined_net_profit",
      coalesce("young"."return_data_total_returns",0) as "_combined_total_returns",
      coalesce(coalesce("charming"."sales_data_sale_entity_id","young"."return_data_return_entity_id"),coalesce("charming"."sales_data_sale_entity_id","young"."return_data_return_entity_id")) as "_combined_eid",
      coalesce(coalesce("charming"."sales_data_schannel","young"."return_data_rchannel"),coalesce("charming"."sales_data_schannel","young"."return_data_rchannel")) as "_combined_chan"
  FROM
      "charming"
      FULL JOIN "young" on "charming"."sales_data_sale_entity_id" = "young"."return_data_return_entity_id" AND "charming"."sales_data_schannel" = "young"."return_data_rchannel"
  GROUP BY
      1,
      2,
      3,
      4,
      5),
  premium as (
  SELECT
      "protective"."_combined_chan" as "combined_chan",
      "protective"."_combined_eid" as "combined_eid",
      "protective"."_combined_gross_sales" as "combined_gross_sales",
      "protective"."_combined_net_profit" as "combined_net_profit",
      "protective"."_combined_total_returns" as "combined_total_returns"
  FROM
      "protective"),
  puzzled as (
  SELECT
      "premium"."combined_chan" as "combined_chan",
      "premium"."combined_eid" as "combined_eid",
      sum("premium"."combined_gross_sales") as "total_gross_sales",
      sum("premium"."combined_net_profit") as "net_profit_amount",
      sum("premium"."combined_total_returns") as "total_returns_amount"
  FROM
      "premium"
  GROUP BY
      ROLLUP (1, 2)),
  waggish as (
  SELECT
      "puzzled"."combined_chan" as "combined_chan",
      "puzzled"."combined_eid" as "combined_eid",
      "puzzled"."net_profit_amount" as "net_profit_amount",
      "puzzled"."total_gross_sales" as "total_gross_sales",
      "puzzled"."total_returns_amount" as "total_returns_amount"
  FROM
      "puzzled"
      LEFT OUTER JOIN "premium" on "puzzled"."combined_chan" = "premium"."combined_chan" AND "puzzled"."combined_eid" = "premium"."combined_eid"),
  rambunctious as (
  SELECT
      "waggish"."net_profit_amount" as "net_profit_amount",
      "waggish"."total_gross_sales" as "total_gross_sales",
      "waggish"."total_returns_amount" as "total_returns_amount",
      grouping("waggish"."combined_chan") as "g_chan",
      grouping("waggish"."combined_eid") as "g_eid"
  FROM
      "waggish"
  GROUP BY
      1,
      2,
      3)
  SELECT
      CASE
  	WHEN "rambunctious"."g_chan" = 1 and "rambunctious"."g_eid" = 1 THEN 'grand total'
  	WHEN "rambunctious"."g_eid" = 1 THEN ("puzzled"."combined_chan" || ' channel')
  	ELSE ("puzzled"."combined_chan" || ' channel')
  	END as "channel_type",
      CASE
  	WHEN "rambunctious"."g_eid" = 1 THEN cast(null as string)
  	WHEN "puzzled"."combined_chan" = 'STORE' THEN ('store' || "puzzled"."combined_eid")
  	WHEN "puzzled"."combined_chan" = 'CATALOG' THEN ('catalog_page' || "puzzled"."combined_eid")
  	WHEN "puzzled"."combined_chan" = 'WEB' THEN ('web_site' || "puzzled"."combined_eid")
  	END as "entity_identifier",
      coalesce("puzzled"."total_gross_sales","rambunctious"."total_gross_sales") as "total_gross_sales",
      coalesce("puzzled"."total_returns_amount","rambunctious"."total_returns_amount") as "total_returns_amount",
      coalesce("puzzled"."net_profit_amount","rambunctious"."net_profit_amount") as "net_profit_amount"
  FROM
      "rambunctious"
      FULL JOIN "puzzled" on "rambunctious"."net_profit_amount" is not distinct from "puzzled"."net_profit_amount" AND "rambunctious"."total_gross_sales" is not distinct from "puzzled"."total_gross_sales" AND "rambunctious"."total_returns_amount" is not distinct from "puzzled"."total_returns_amount"
  ORDER BY
      CASE
  	"puzzled"."combined_chan"
  	WHEN 'STORE' THEN 1
  	WHEN 'CATALOG' THEN 2
  	WHEN 'WEB' THEN 3
  	ELSE 4
  	END asc,
      "puzzled"."combined_eid" asc
  LIMIT (500)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: 3 undefined concept references; fix all before re-running:
    - first_name (line 50, col 49, in ORDER BY); did you mean: all_sales.billing_customer.first_name, all_sales.ship_customer.first_name, all_sales.purchasing_customer.first_name?
    - last_name (line 50, col 77, in ORDER BY); did you mean: all_sales.billing_customer.last_name, all_sales.ship_customer.last_name, all_sales.purchasing_customer.last_name?
    - preferred_cust_flag (line 50, col 104, in ORDER BY); did you mean: all_sales.billing_customer.preferred_cust_flag, all_sales.ship_customer.preferred_cust_flag, all_sales.purchasing_customer.preferred_cust_flag?
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 46 column 12 (char 1839). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query14.preql duckdb`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 19). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14_check6.preql duckdb`

  ```text
  Syntax error in query14_check6.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 14). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query17.preql --import raw.date:date select distinct date.quarter_name from date;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<cust_totals.cust_id,cust_totals.total_spent>': None} and need cust_totals.total_spent > multiply(0.5,local.max_spent@Grain<Abstract>)
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'RowsetNode<cust_totals.cust_id,cust_totals.total_spent>': None} and need cust_totals.total_spent > multiply(0.5,max_cust.max_total@Grain<Abstract>)
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: Have {'MergeNode<local._virt_agg_sum_8266135611007852,store.customer.id,store.date.year>': None} and need store.date.year@Grain<store.date.id> between 2000 and 2003 and store.customer.id is not MagicConstants.NULL and local._virt_agg_sum_8266135611007852 > multiply(0.5,local.max_total@Grain<Abstract>)
  ```
- `trilogy run raw/date.preql duckdb --import raw.date:date select date.month_seq where date.year = 1998 and date.month_of_year = 12 limit 1;`

  ```text
  --import only applies to inline queries, not file/directory inputs.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {cs.item.color, cs.item.current_price, local._items_qual_item_id, local._items_qual_item_name, local.cat_list_by_item_order}; {local.refund_by_item_order}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 64 column 105 (char 2256). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query75.preql duckdb`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.brand_id, item.category, item.category_id, item.class_id, item.manufacturer_id}; {local.item_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 48 column 12 (char 2032). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: Undefined concept: wr.item.id. Suggestions: ['wr.web_sales.item.id', 'ss.item.id', 'ss.date.id', 'ss.return_date.id', 'ss.time.id', 'ss.return_time.id']
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/all_sales:sales select sales.date.week_seq, sales.date.day_of_week, sales.date.year, sum(sales.ext_sales_price) as total_esp where s…WEB', 'CATALOG') group by sales.date.week_seq, sales.date.day_of_week, sales.date.year order by sales.date.week_seq, sales.date.day_of_week limit 30;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...channel in ('WEB', 'CATALOG') ??? group by sales.date.week_seq,
  ```
- `trilogy file write query02.preql --content import raw/all_sales as sales;

# Aggregate weekly sales by day of week for web and catalog channels
with weekly_s…s_thu / nxt_thu, 2) as thu,
    round(ws_fri / nxt_fri, 2) as fri,
    round(ws_sat / nxt_sat, 2) as sat
order by weekly_sales.week_seq nulls first;
`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/all_sales as sales;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/all_sales as sales;  # Agg...

  Write stats: received 3890 chars / 3890 bytes; tail: …'_sat, 2) as sat\\norder by weekly_sales.week_seq nulls first;\\n'.
  ```
- `trilogy file write query02.preql -e -c import raw.all_sales as sales;

# Aggregate weekly sales by day of week for web and catalog channels
with weekly_dow a…
    round(next_vals.nxt_sat / cur_vals.sat_sales, 2) as sat
having next_vals.week_seq is not null
order by next_vals.week_seq nulls first
limit 200;`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...1 where next_vals.week_seq in ??? (select weekly_dow.week_seq wh...

  Write stats: received 2922 chars / 2922 bytes; tail: …' not null\\norder by next_vals.week_seq nulls first\\nlimit 200;'.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, count(all_sales.row_one) as cnt group by all_sales.channel, all_sales.channel_dim_text_id limit 5;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...unt(all_sales.row_one) as cnt ??? group by all_sales.channel, al...
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 20:9
     |
  20 |         -sum(all_sales.return_net_loss) as net_profit
     |         ^---
     |
     = expected select_item, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...nt) as total_returns,         ??? -sum(all_sales.return_net_loss...
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:65
     |
  22 | ) -> (channel, entity_id, gross_sales, total_returns, net_profit)
     |                                                                 ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...les, total_returns, net_profit ??? ) select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:97
     |
  22 | ) -> (channel string, entity_id string, gross_sales float, total_returns float, net_profit float)
     |                                                                                                 ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
  ...eturns float, net_profit float ??? ) select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:65
     |
  22 | ) -> (channel, entity_id, gross_sales, total_returns, net_profit)
     |                                                                 ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...les, total_returns, net_profit ??? ) select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 23:1
     |
  23 | select
     | ^---
     |
     = expected tvf_output
  Location:
  ...t_loss) as net_profit     ) ) ??? select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:65
     |
  22 | ) -> (channel, entity_id, gross_sales, total_returns, net_profit)
     |                                                                 ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...les, total_returns, net_profit ??? ) select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:97
     |
  22 | ) -> (channel string, entity_id string, gross_sales float, total_returns float, net_profit float)
     |                                                                                                 ^---
     |
     = expected concept_nullable_modifier or metadata
  Location:
  ...eturns float, net_profit float ??? ) select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:65
     |
  22 | ) -> (channel, entity_id, gross_sales, total_returns, net_profit)
     |                                                                 ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...les, total_returns, net_profit ??? ) select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:65
     |
  22 | ) -> (channel, entity_id, gross_sales, total_returns, net_profit)
     |                                                                 ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...les, total_returns, net_profit ??? ) select     stacked.channel,
  ```
- `trilogy run --import raw/all_sales:all_sales 
with stacked as union(
    (where all_sales.date.date between '2000-08-23'::date and '2000-09-06'::date
       … sum(stacked.gross_sales) as total_gross_sales,
    sum(stacked.total_returns) as total_returns,
    sum(stacked.net_profit) as net_profit
limit 10;
`

  ```text
  Syntax error in stdin:   --> 22:65
     |
  22 | ) -> (channel, entity_id, gross_sales, total_returns, net_profit)
     |                                                                 ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ...les, total_returns, net_profit ??? ) select     stacked.channel,
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Sales data: aggregate by channel + sale store identifier
with sales_data as
…'catalog_page' || c.entity_id
        when c.channel = 'WEB' then 'web_site' || c.entity_id
    end as entity_identifier
from combined as c
limit 10;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
       end as entity_identifier ??? from combined as c limit 10;

  Write stats: received 2009 chars / 2009 bytes; tail: …'id\\n    end as entity_identifier\\nfrom combined as c\\nlimit 10;'.
  ```
- `trilogy file write query14.preql --content import raw.all_sales as sales;

# Overall average sale value across all channels 1999-2001 (scalar)
auto overall_a…rics.channel nulls first,
  base_metrics.brand_id nulls first,
  base_metrics.class_id nulls first,
  base_metrics.category_id nulls first
limit 100;`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:1
     |
  10 | rowset qualified_keys as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ...l 3 channels during 1999-2001 ??? rowset qualified_keys as where...

  Write stats: received 1835 chars / 1835 bytes; tail: …'lls first,\\n  base_metrics.category_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query14_check2.preql --content import raw.all_sales as sales;

auto overall_avg <- sum(sales.quantity * sales.list_price ? sales.date.year… category_id
having
  total_sales > overall_avg
order by
  sales.channel, sales.item.brand_id, sales.item.class_id, sales.item.category_id
limit 100;`

  ```text
  refused to write 'query14_check2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ombo_key in qualified_keys.ck ??? group by channel, brand_id, cl...

  Write stats: received 986 chars / 986 bytes; tail: …'d_id, sales.item.class_id, sales.item.category_id\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store;
import raw.catalog_sales as cat;
import raw.web_sales as web;

# Step 1: Frequent…2000_combined.last_name asc nulls first,
    feb2000_combined.first_name asc nulls first,
    feb2000_combined.total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | rowset frequent_desc_prefixes as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...e pairs in 2000-2003 exceed 4 ??? rowset frequent_desc_prefixes

  Write stats: received 2344 chars / 2344 bytes; tail: …'\\n    feb2000_combined.total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store;
import raw.catalog_sales as cat;
import raw.web_sales as web;

# Step 1: Frequent…2000_combined.last_name asc nulls first,
    feb2000_combined.first_name asc nulls first,
    feb2000_combined.total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 57:41
     |
  57 | ) -> (last_name, first_name, total_sales)
     |                                         ^---
     |
     = expected metadata, PURPOSE, or data_type
  Location:
  ..._name, first_name, total_sales ??? )  # Final output per customer...

  Write stats: received 2343 chars / 2343 bytes; tail: …'\\n    feb2000_combined.total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as store;
import raw.catalog_sales as cat;
import raw.web_sales as web;

# Step 1: Frequent…2000_combined.last_name asc nulls first,
    feb2000_combined.first_name asc nulls first,
    feb2000_combined.total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 39:1
     |
  39 | select
     | ^---
     |
     = expected limit, order_by, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...cust_totals > 0.5 * max_total ??? select     cust_id ;  # Step 3...

  Write stats: received 2570 chars / 2570 bytes; tail: …'\\n    feb2000_combined.total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…) as line_cnt,
    sum(ss.ext_wholesale_cost) as total_wholesale,
    sum(ss.ext_list_price) as total_list,
    sum(ss.coupon_amt) as total_coupon
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 63:17
     |
  63 |   and (date.year(ss.date) = 1999 or date.year(ss.date) = 2000)
     |                 ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...arital_status   and (date.year ??? (ss.date) = 1999 or date.year(...

  Write stats: received 3614 chars / 3614 bytes; tail: …'ce) as total_list,\\n    sum(ss.coupon_amt) as total_coupon\\n;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…d yr2000.line_cnt <= yr1999.line_cnt
order by
    yr1999.prod_name,
    yr1999.store_name,
    yr2000.line_cnt,
    yr1999.tot_w,
    yr2000.tot_w
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...urned and demographics differ ??? rowset yr1999 as where ss.item...

  Write stats: received 3923 chars / 3923 bytes; tail: …',\\n    yr2000.line_cnt,\\n    yr1999.tot_w,\\n    yr2000.tot_w\\n;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…d yr2000.line_cnt <= yr1999.line_cnt
order by
    yr1999.prod_name,
    yr1999.store_name,
    yr2000.line_cnt,
    yr1999.tot_w,
    yr2000.tot_w
;
`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | rowset yr1999 as
     | ^---
     |
     = expected EOI, block, or show_statement
  Location:
  ....product_name as item_name ;  ??? rowset yr1999 as where ss.item...

  Write stats: received 3776 chars / 3776 bytes; tail: …',\\n    yr2000.line_cnt,\\n    yr1999.tot_w,\\n    yr2000.tot_w\\n;\\n'.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ..._zip = yr2000.store_zip   and ??? yr2000.line_cnt <= yr1999.line...
  ```
- `trilogy file write query75.preql --content import raw.all_sales as sales;
import raw.item as item;
import raw.date as date;

where item.category = 'Books'
select
    date.year,
    item.brand_id,
    item.class_id,
    item.category_id,
    item.manufacturer_id,
    sales.item.id as item_id
`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
       sales.item.id as item_id ???

  Write stats: received 249 chars / 249 bytes; tail: …'_id,\\n    item.manufacturer_id,\\n    sales.item.id as item_id\\n'.
  ```
- `trilogy file write query75.preql`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 41:27
     |
  41 | where by_year.year = 2001 as prev
     |                           ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ff
   where by_year.year = 2001 ??? as prev
   inner join by_year.ye...

  Write stats: received 1746 chars / 1746 bytes; tail: …'\\norder by\r\\n    qty_diff asc,\r\\n    amt_diff asc\r\\nlimit 100;\r\\n'.
  ```
- `trilogy file write query75.preql`

  ```text
  refused to write 'query75.preql': not syntactically valid Trilogy.

  Parse error:
    --> 40:32
     |
  40 | inner join by_year.year = 2001 as prev
     |                                ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...nner join by_year.year = 2001 ??? as prev
   inner join by_year.ye...

  Write stats: received 1694 chars / 1694 bytes; tail: …'\\norder by\r\\n    qty_diff asc,\r\\n    amt_diff asc\r\\nlimit 100;\r\\n'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.date:date select distinct date.quarter_name from date limit 20;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct date.quarter_name from date as distinct_date_quarter_name_from_date`
  Location:
  ...date as date; select distinct ??? date.quarter_name from date li...
  ```
- `trilogy file write _test.preql --content import raw.date as d;
select distinct d.quarter_name limit 20;`

  ```text
  refused to write '_test.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct d.quarter_name as distinct_d_quarter_name`
  Location:
  ...aw.date as d; select distinct ??? d.quarter_name limit 20;

  Write stats: received 62 chars / 62 bytes; tail: …'port raw.date as d;\\nselect distinct d.quarter_name limit 20;'.
  ```
- `trilogy run --import raw.catalog_sales:cs select count(cs.line_item) where cs.sold_date.year = 1998 and cs.sold_date.month_of_year = 12 and cs.item.category = 'Women' and cs.item.class = 'maternity' limit 1; duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(cs.line_item) as line_item_count`
  Location:
  ...s; select count(cs.line_item) ??? where cs.sold_date.year = 1998...
  ```
- `trilogy run --import raw.web_sales:ws select count(ws.line_item) where ws.date.year = 1998 and ws.date.month_of_year = 12 and ws.item.category = 'Women' and ws.item.class = 'maternity' limit 1; duckdb`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ws.line_item) as line_item_count`
  Location:
  ...s; select count(ws.line_item) ??? where ws.date.year = 1998 and
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```

### `type-error`

- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Invalid argument type 'Trait<INTEGER, ['year']>' passed into YEAR function in position 1 from concept: ss.date.year. Valid: 'DATE', 'DATETIME', 'TIMESTAMP'.
  ```
