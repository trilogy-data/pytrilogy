# Trilogy failure analysis — 20260623-010828

- Run `20260623-010828` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 366 | failed: 66 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 36 | 55% |
| `syntax-parse` | 21 | 32% |
| `syntax-missing-alias` | 4 | 6% |
| `cli-misuse` | 3 | 5% |
| `join-resolution` | 1 | 2% |
| `file-not-found` | 1 | 2% |

## Detail

### `other`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/all_sales:all_sales select all_sales.channel, all_sales.channel_dim_text_id, all_sales.return_channel_dim_text_id, all_sales.is_returned, all_sales.net_profit, all_sales.return_net_loss, all_sales.return_amount limit 10;`

  ```text
  Unexpected error in stdin: (_duckdb.OutOfMemoryException) Out of Memory Error: could not allocate block of size 256.0 KiB (24.9 GiB/25.0 GiB used)

  Possible solutions:
  * Reducing the number of threads (SET threads=X)
  * Disabling insertion-order preservation (SET preserve_insertion_order=false)
  * Increasing the memory limit (SET memory_limit='...GB')

  See also https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_dim_return_unified"."CP_CATALOG_PAGE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "catalog_page" as "all_sales_catalog_dim_return_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_dim_return_unified"."S_STORE_ID" as "all_sales_return_channel_dim_text_id"
  FROM
      "store" as "all_sales_store_dim_return_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_dim_return_unified"."web_site_id" as "all_sales_return_channel_dim_text_id"
  FROM
      "web_site" as "all_sales_web_dim_return_unified"),
  uneven as (
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
  concerned as (
  SELECT
       'CATALOG'  as "all_sales_channel",
       true  as "all_sales_is_returned",
      "all_sales_catalog_returns_unified"."CR_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "all_sales_return_amount",
      "all_sales_catalog_returns_unified"."CR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "catalog_returns" as "all_sales_catalog_returns_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
       true  as "all_sales_is_returned",
      "all_sales_store_returns_unified"."SR_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_returns_unified"."SR_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_returns_unified"."SR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_store_returns_unified"."SR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "store_returns" as "all_sales_store_returns_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
       true  as "all_sales_is_returned",
      "all_sales_web_returns_unified"."WR_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_returns_unified"."WR_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_returns_unified"."WR_RETURN_AMT" as "all_sales_return_amount",
      "all_sales_web_returns_unified"."WR_NET_LOSS" as "all_sales_return_net_loss"
  FROM
      "web_returns" as "all_sales_web_returns_unified"),
  sweltering as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_CATALOG_PAGE_SK" as "all_sales_channel_dim_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_sales_unified"."CS_NET_PROFIT" as "all_sales_net_profit",
      "all_sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "all_sales_order_id"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_STORE_SK" as "all_sales_channel_dim_id",
      "all_sales_store_sales_unified"."SS_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_sales_unified"."SS_NET_PROFIT" as "all_sales_net_profit",
      "all_sales_store_sales_unified"."SS_TICKET_NUMBER" as "all_sales_order_id"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_WEB_SITE_SK" as "all_sales_channel_dim_id",
      "all_sales_web_sales_unified"."WS_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_sales_unified"."WS_NET_PROFIT" as "all_sales_net_profit",
      "all_sales_web_sales_unified"."WS_ORDER_NUMBER" as "all_sales_order_id"
  FROM
      "web_sales" as "all_sales_web_sales_unified"),
  thoughtful as (
  SELECT
      "cheerful"."all_sales_channel" as "all_sales_channel",
      "cheerful"."all_sales_return_channel_dim_text_id" as "all_sales_return_channel_dim_text_id"
  FROM
      "cheerful"
  GROUP BY
      1,
      2)
  SELECT
      "thoughtful"."all_sales_channel" as "all_sales_channel",
      "uneven"."all_sales_channel_dim_text_id" as "all_sales_channel_dim_text_id",
      "thoughtful"."all_sales_return_channel_dim_text_id" as "all_sales_return_channel_dim_text_id",
      "concerned"."all_sales_is_returned" as "all_sales_is_returned",
      "sweltering"."all_sales_net_profit" as "all_sales_net_profit",
      "concerned"."all_sales_return_net_loss" as "all_sales_return_net_loss",
      "concerned"."all_sales_return_amount" as "all_sales_return_amount"
  FROM
      "sweltering"
      LEFT OUTER JOIN "concerned" on "sweltering"."all_sales_channel" = "concerned"."all_sales_channel" AND "sweltering"."all_sales_item_id" = "concerned"."all_sales_item_id" AND "sweltering"."all_sales_order_id" = "concerned"."all_sales_order_id"
      LEFT OUTER JOIN "uneven" on "sweltering"."all_sales_channel" = "uneven"."all_sales_channel" AND "sweltering"."all_sales_channel_dim_id" = "uneven"."all_sales_channel_dim_id"
      INNER JOIN "thoughtful" on "sweltering"."all_sales_channel" = "thoughtful"."all_sales_channel"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7
  LIMIT (10)]
  (Background on this error at: https://sqlalche.me/e/20/e3q8)
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: all_sales.entity_prefix. Suggestions: ['entity_prefix', 'all_sales.net_profit', 'all_sales.ext_list_price', 'all_sales.list_price']
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(combined.channel_label)` (line 48), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(combined.channel_label) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept local.g1 with grain Grain<local.channel_type,local.entity_identifier,local.net_profit,local.total_gross_sales,local.total_returns_amount> and lineage grouping(ref:combined.channel_label)<['ref:local.channel_type', 'ref:local.entity_identifier', 'ref:local.total_gross_sales', 'ref:local.total_returns_amount', 'ref:local.net_profit']>. This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: maximum recursion depth exceeded
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept local.channel_type with grain Grain<Abstract> and lineage UnionSelectLineage(selects=[SelectLineage(selection=[ref:local.___tvf_arm_0_channel_type, ref:local.___tvf_arm_0_entity_identifier, ref:local.___tvf_arm_0_total_gross_sales, ref:local.___tvf_arm_0_total_returns_amount, ref:local.___tvf_arm_0_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_0_channel_type': local.___tvf_arm_0_channel_type@Grain<Abstract>, 'local.___tvf_arm_0_entity_identifier': local.___tvf_arm_0_entity_identifier@Grain<all_sales.channel_dim_id>, 'local.___tvf_arm_0_total_gross_sales': local.___tvf_arm_0_total_gross_sales@Grain<local.___tvf_arm_0_channel_type,local.___tvf_arm_0_entity_identifier>, 'local.___tvf_arm_0_total_returns_amount': local.___tvf_arm_0_total_returns_amount@Grain<Abstract>, 'local.___tvf_arm_0_net_profit': local.___tvf_arm_0_net_profit@Grain<local.___tvf_arm_0_channel_type,local.___tvf_arm_0_entity_identifier>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'local.___tvf_arm_0_channel_type', 'local.___tvf_arm_0_entity_identifier'}, where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, component_order=['local.___tvf_arm_0_channel_type', 'local.___tvf_arm_0_entity_identifier']), where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:local.___tvf_arm_1_channel_type, ref:local.___tvf_arm_1_entity_identifier, ref:local.___tvf_arm_1_total_gross_sales, ref:local.___tvf_arm_1_total_returns_amount, ref:local.___tvf_arm_1_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_1_channel_type': local.___tvf_arm_1_channel_type@Grain<Abstract>, 'local.___tvf_arm_1_entity_identifier': local.___tvf_arm_1_entity_identifier@Grain<all_sales.return_channel_dim_id>, 'local.___tvf_arm_1_total_gross_sales': local.___tvf_arm_1_total_gross_sales@Grain<Abstract>, 'local.___tvf_arm_1_total_returns_amount': local.___tvf_arm_1_total_returns_amount@Grain<local.___tvf_arm_1_channel_type,local.___tvf_arm_1_entity_identifier>, 'local.___tvf_arm_1_net_profit': local.___tvf_arm_1_net_profit@Grain<all_sales.channel,all_sales.item.id,all_sales.order_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'local.___tvf_arm_1_entity_identifier', 'local.___tvf_arm_1_channel_type'}, where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, component_order=['local.___tvf_arm_1_channel_type', 'local.___tvf_arm_1_entity_identifier']), where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:local.___tvf_arm_2_channel_type, ref:local.___tvf_arm_2_entity_identifier, ref:local.___tvf_arm_2_total_gross_sales, ref:local.___tvf_arm_2_total_returns_amount, ref:local.___tvf_arm_2_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_2_channel_type': local.___tvf_arm_2_channel_type@Grain<Abstract>, 'local.___tvf_arm_2_entity_identifier': local.___tvf_arm_2_entity_identifier@Grain<Abstract>, 'local.___tvf_arm_2_total_gross_sales': local.___tvf_arm_2_total_gross_sales@Grain<local.___tvf_arm_2_channel_type>, 'local.___tvf_arm_2_total_returns_amount': local.___tvf_arm_2_total_returns_amount@Grain<Abstract>, 'local.___tvf_arm_2_net_profit': local.___tvf_arm_2_net_profit@Grain<local.___tvf_arm_2_channel_type>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'local.___tvf_arm_2_channel_type'}, where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, component_order=['local.___tvf_arm_2_channel_type']), where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:local.___tvf_arm_3_channel_type, ref:local.___tvf_arm_3_entity_identifier, ref:local.___tvf_arm_3_total_gross_sales, ref:local.___tvf_arm_3_total_returns_amount, ref:local.___tvf_arm_3_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_3_channel_type': local.___tvf_arm_3_channel_type@Grain<Abstract>, 'local.___tvf_arm_3_entity_identifier': local.___tvf_arm_3_entity_identifier@Grain<Abstract>, 'local.___tvf_arm_3_total_gross_sales': local.___tvf_arm_3_total_gross_sales@Grain<Abstract>, 'local.___tvf_arm_3_total_returns_amount': local.___tvf_arm_3_total_returns_amount@Grain<local.___tvf_arm_3_channel_type>, 'local.___tvf_arm_3_net_profit': local.___tvf_arm_3_net_profit@Grain<all_sales.channel,all_sales.item.id,all_sales.order_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'local.___tvf_arm_3_channel_type'}, where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, component_order=['local.___tvf_arm_3_channel_type']), where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:local.___tvf_arm_4_channel_type, ref:local.___tvf_arm_4_entity_identifier, ref:local.___tvf_arm_4_total_gross_sales, ref:local.___tvf_arm_4_total_returns_amount, ref:local.___tvf_arm_4_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_4_channel_type': local.___tvf_arm_4_channel_type@Grain<Abstract>|ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, 'local.___tvf_arm_4_entity_identifier': local.___tvf_arm_4_entity_identifier@Grain<Abstract>, 'local.___tvf_arm_4_total_gross_sales': local.___tvf_arm_4_total_gross_sales@Grain<Abstract>|ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, 'local.___tvf_arm_4_total_returns_amount': local.___tvf_arm_4_total_returns_amount@Grain<Abstract>, 'local.___tvf_arm_4_net_profit': local.___tvf_arm_4_net_profit@Grain<Abstract>|ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components=set(), where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, component_order=[]), where_clause=ref:all_sales.date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:local.___tvf_arm_5_channel_type, ref:local.___tvf_arm_5_entity_identifier, ref:local.___tvf_arm_5_total_gross_sales, ref:local.___tvf_arm_5_total_returns_amount, ref:local.___tvf_arm_5_net_profit], hidden_components=set(), local_concepts={'local.___tvf_arm_5_channel_type': local.___tvf_arm_5_channel_type@Grain<Abstract>|ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, 'local.___tvf_arm_5_entity_identifier': local.___tvf_arm_5_entity_identifier@Grain<Abstract>, 'local.___tvf_arm_5_total_gross_sales': local.___tvf_arm_5_total_gross_sales@Grain<Abstract>, 'local.___tvf_arm_5_total_returns_amount': local.___tvf_arm_5_total_returns_amount@Grain<Abstract>|ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, 'local.___tvf_arm_5_net_profit': local.___tvf_arm_5_net_profit@Grain<all_sales.channel,all_sales.item.id,all_sales.order_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components=set(), where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, component_order=[]), where_clause=ref:all_sales.return_date.date between constant(2000-08-23) and constant(2000-09-06) and ref:all_sales.return_channel_dim_id is not MagicConstants.NULL, having_clause=None, scoped_joins=[])], align=AlignClause(items=[AlignItem(alias='channel_type', concepts=[ref:local.___tvf_arm_0_channel_type, ref:local.___tvf_arm_1_channel_type, ref:local.___tvf_arm_2_channel_type, ref:local.___tvf_arm_3_channel_type, ref:local.___tvf_arm_4_channel_type, ref:local.___tvf_arm_5_channel_type], namespace='local', hidden=False), AlignItem(alias='entity_identifier', concepts=[ref:local.___tvf_arm_0_entity_identifier, ref:local.___tvf_arm_1_entity_identifier, ref:local.___tvf_arm_2_entity_identifier, ref:local.___tvf_arm_3_entity_identifier, ref:local.___tvf_arm_4_entity_identifier, ref:local.___tvf_arm_5_entity_identifier], namespace='local', hidden=False), AlignItem(alias='total_gross_sales', concepts=[ref:local.___tvf_arm_0_total_gross_sales, ref:local.___tvf_arm_1_total_gross_sales, ref:local.___tvf_arm_2_total_gross_sales, ref:local.___tvf_arm_3_total_gross_sales, ref:local.___tvf_arm_4_total_gross_sales, ref:local.___tvf_arm_5_total_gross_sales], namespace='local', hidden=False), AlignItem(alias='total_returns_amount', concepts=[ref:local.___tvf_arm_0_total_returns_amount, ref:local.___tvf_arm_1_total_returns_amount, ref:local.___tvf_arm_2_total_returns_amount, ref:local.___tvf_arm_3_total_returns_amount, ref:local.___tvf_arm_4_total_returns_amount, ref:local.___tvf_arm_5_total_returns_amount], namespace='local', hidden=False), AlignItem(alias='net_profit', concepts=[ref:local.___tvf_arm_0_net_profit, ref:local.___tvf_arm_1_net_profit, ref:local.___tvf_arm_2_net_profit, ref:local.___tvf_arm_3_net_profit, ref:local.___tvf_arm_4_net_profit, ref:local.___tvf_arm_5_net_profit], namespace='local', hidden=False)]), namespace='local', hidden_components=set(), order_by=None, limit=None, where_clause=None, having_clause=None, derive=None). This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept combined.entity_id with grain Grain<Abstract> and lineage <Rowset<combined>: ref:local.entity_id>. This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept combined.entity_id with grain Grain<Abstract> and lineage <Rowset<combined>: ref:local.entity_id>. This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Unexpected error in query05.preql: Recursion error building concept combined.entity_id with grain Grain<Abstract> and lineage <Rowset<combined>: ref:local.entity_id>. This is likely due to a circular reference.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Output column 'net_profit' aliases 'combined.net_profit', which is itself the 'net_profit' output of a union(...)/rowset, so the rename refers back to itself. Use a distinct output name (e.g. 'net_profit_out').
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query23.preql`

  ```text
  Syntax error in query23.preql: HAVING references 'cust_store_totals.cust_total', 'local.global_max_total', which are not in the SELECT projection (line 29). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --cust_store_totals.cust_total, --local.global_max_total
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query29.preql`

  ```text
  Resolution error in query29.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {local.catalog_sales_qty}; {local.store_return_qty, local.store_sales_qty, store_sales.item.desc, store_sales.item.text_id, store_sales.store.name, store_sales.store.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: qualifying_items.item_id (line 38, col 26, in WHERE). Suggestions: ['qualifying_items.cat_sales_by_item.item_id', 'cat_sales_by_item.item_id', 'cat_returns_by_item.item_id', 'items_with_store_returns.item_id', 'item_id', '_cat_sales_by_item_item_id']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {local.cat_ext_list_by_item}; {local.cat_refund_by_item}; {local.cnt, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_ship_year, local.item_id, local.product_name, local.sale_city, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.total_coupon_amt, local.total_list_price, local.total_wholesale_cost, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status, ss.item.color, ss.item.current_price, ss.item.text_id}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: qualify_items.item_id (line 40, col 26, in WHERE). Suggestions: ['qualify_items.cat_sales_agg.item_id', 'store_return_items.item_id', 'cat_sales_agg.item_id', 'cat_returns_agg.item_id', 'item_id']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {local._store_return_items_item_id}; {pr.item.id, pr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Have {'GroupNode<cat_returns_agg.item_id,cat_returns_agg.item_id,cat_returns_agg.total_cat_refund>': None} and need cat_sales_agg.total_cat_list_price > multiply(2,cat_returns_agg.total_cat_refund@Grain<cat_returns_agg.item_id>)
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: HAVING references 'qualify_items.total_cat_list_price', 'qualify_items.total_cat_refund', which are not in the SELECT projection (line 29). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --qualify_items.total_cat_list_price, --qualify_items.total_cat_refund
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {cat_returns_agg.total_cat_refund}; {cat_sales_agg.total_cat_list_price}; {local.cnt, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_ship_year, local.item_id, local.product_name, local.sale_city, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.total_coupon_amt, local.total_list_price, local.total_wholesale_cost}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Invalid reference string found in query:
  WITH
  wakeful as (
  SELECT
      INVALID_REFERENCE_BUG as "qualify_items_cat_sales_agg_item_id"
  FROM
      "catalog_returns" as "cr_catalog_returns"
      INNER JOIN "item" as "cr_item_items" on "cr_catalog_returns"."CR_ITEM_SK" = "cr_item_items"."I_ITEM_SK"
  GROUP BY
      1),
  sweltering as (
  SELECT
      "ss_customer_address_customer_address"."CA_CITY" as "ss_customer_address_city",
      "ss_customer_address_customer_address"."CA_STREET_NAME" as "ss_customer_address_street_name",
      "ss_customer_address_customer_address"."CA_STREET_NUMBER" as "ss_customer_address_street_number",
      "ss_customer_address_customer_address"."CA_ZIP" as "ss_customer_address_zip",
      "ss_customer_first_sales_date_date"."D_YEAR" as "ss_customer_first_sales_date_year",
      "ss_customer_first_shipto_date_date"."D_YEAR" as "ss_customer_first_shipto_date_year",
      "ss_date_date"."D_YEAR" as "ss_date_year",
      "ss_item_items"."I_ITEM_ID" as "ss_item_text_id",
      "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
      "ss_sale_address_customer_address"."CA_CITY" as "ss_sale_address_city",
      "ss_sale_address_customer_address"."CA_STREET_NAME" as "ss_sale_address_street_name",
      "ss_sale_address_customer_address"."CA_STREET_NUMBER" as "ss_sale_address_street_number",
      "ss_sale_address_customer_address"."CA_ZIP" as "ss_sale_address_zip",
      "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
      "ss_store_sales"."SS_EXT_LIST_PRICE" as "ss_ext_list_price",
      "ss_store_sales"."SS_EXT_WHOLESALE_COST" as "ss_ext_wholesale_cost",
      "ss_store_store"."S_STORE_NAME" as "ss_store_name",
      "ss_store_store"."S_ZIP" as "ss_store_zip",
      (cast("ss_store_sales"."SS_TICKET_NUMBER" as string) || '-' || cast("ss_item_items"."I_ITEM_SK" as string)) as "ss_line_item"
  FROM
      "store_sales" as "ss_store_sales"
      LEFT OUTER JOIN "date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
      INNER JOIN "customer" as "ss_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_customer_customers"."C_CUSTOMER_SK"
      LEFT OUTER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
      LEFT OUTER JOIN "customer_address" as "ss_sale_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_sale_address_customer_address"."CA_ADDRESS_SK"
      LEFT OUTER JOIN "customer_address" as "ss_customer_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
      INNER JOIN "customer_demographics" as "ss_customer_demographics_customer_demographics" on "ss_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_customer_demographics_customer_demographics"."CD_DEMO_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date" on "ss_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_customer_first_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date" on "ss_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_customer_first_shipto_date_date"."D_DATE_SK"
      INNER JOIN "item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
  WHERE
      "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_item_items"."I_ITEM_ID" in (select wakeful."qualify_items_cat_sales_agg_item_id" from wakeful where wakeful."qualify_items_cat_sales_agg_item_id" is not null) and SR_RETURN_TIME_SK IS NOT NULL = True and "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_customer_demographics_customer_demographics"."CD_MARITAL_STATUS"

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      "ss_item_items"."I_ITEM_SK",
      "ss_store_sales"."SS_TICKET_NUMBER")
  SELECT
      "sweltering"."ss_item_product_name" as "product_name",
      "sweltering"."ss_item_text_id" as "item_id",
      "sweltering"."ss_store_name" as "store_name",
      "sweltering"."ss_store_zip" as "store_zip",
      "sweltering"."ss_sale_address_street_number" as "sale_street_number",
      "sweltering"."ss_sale_address_street_name" as "sale_street_name",
      "sweltering"."ss_sale_address_city" as "sale_city",
      "sweltering"."ss_sale_address_zip" as "sale_zip",
      "sweltering"."ss_customer_address_street_number" as "cust_street_number",
      "sweltering"."ss_customer_address_street_name" as "cust_street_name",
      "sweltering"."ss_customer_address_city" as "cust_city",
      "sweltering"."ss_customer_address_zip" as "cust_zip",
      "sweltering"."ss_date_year" as "sale_year",
      "sweltering"."ss_customer_first_sales_date_year" as "first_sales_year",
      "sweltering"."ss_customer_first_shipto_date_year" as "first_ship_year",
      count("sweltering"."ss_line_item") as "cnt",
      sum("sweltering"."ss_ext_wholesale_cost") as "total_wholesale_cost",
      sum("sweltering"."ss_ext_list_price") as "total_list_price",
      sum("sweltering"."ss_coupon_amt") as "total_coupon_amt"
  FROM
      "sweltering"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15
  ORDER BY
      "product_name" asc,
      "store_name" asc
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: yr1999.iid. Suggestions: ['yr1999.agg_sales.iid', 'agg_sales.iid', 'yr2000.agg_sales.iid', 'yr1999.szip', 'yr1999.cnt', 'yr1999_list']
  ```
- `trilogy run query78.preql`

  ```text
  Resolution error in query78.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_agg.catalog_qty, catalog_agg.catalog_sales_price, catalog_agg.catalog_wholesale_cost, catalog_agg.customer_id, catalog_agg.item_id, catalog_agg.year, store_agg.store_qty, store_agg.store_sales_price, store_agg.store_wholesale_cost, web_agg.web_qty, web_agg.web_sales_price, web_agg.web_wholesale_cost}
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.web_sales:web_sales --import raw.catalog_sales:catalog_sales 
rowset store_agg <- where store_s… web_agg.item_id
left join store_agg.customer_id = web_agg.customer_id
where web_agg.web_qty is not null
order by store_agg.store_qty desc
limit 20;
`

  ```text
  Syntax error in stdin: Undefined concept: web_sales.customer.id. Suggestions: ['store_sales.item.id', 'store_sales.date.id', 'store_sales.return_date.id', 'store_sales.time.id', 'store_sales.return_time.id', 'store_sales.customer.demographics.id']
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.web_sales:web_sales --import raw.catalog_sales:catalog_sales 
auto store_qty <- sum(store_sales…store_sales.item.id
inner join web_sales.billing_customer.id = store_sales.customer.id
where web_qty is not null and store_qty is not null
limit 10;
`

  ```text
  Resolution error in stdin: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {local.store_qty, local.store_sales_price, local.store_wholesale, local.web_qty, local.web_sales_price, local.web_wholesale, store_sales.customer.id, store_sales.date.year, store_sales.item.id}
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.web_sales:web_sales --import raw.catalog_sales:catalog_sales 
rowset store_agg <- 
where store_…ar
inner join store_agg.item_id = web_agg.item_id
inner join store_agg.customer_id = web_agg.customer_id
where catalog_agg.catalog_qty > 0
limit 10;
`

  ```text
  Resolution error in stdin: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {catalog_agg.catalog_qty, catalog_agg.catalog_sales_price, catalog_agg.catalog_wholesale_cost, web_agg.customer_id, web_agg.item_id, web_agg.web_qty, web_agg.web_sales_price, web_agg.web_wholesale_cost, web_agg.year}
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query80.preql`

  ```text
  Syntax error in query80.preql: Undefined concept: base.net_profit (line 25, in SELECT). Suggestions: ['base.sales.net_profit', 'sales.net_profit', 'sales.net_paid']
  ```
- `trilogy file read raw/physical_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query83.preql`

  ```text
  Unexpected error in query83.preql: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 71:     bool_or("questionable"."s_channel" = 'STORE') = True and...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
       true  as "s_is_returned",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
      INNER JOIN "date_dim" as "s_return_date_date" on "s_catalog_returns_unified"."CR_RETURNED_DATE_SK" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "s_return_date_date"."D_WEEK_SEQ" in $1

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
       true  as "s_is_returned",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "store_returns" as "s_store_returns_unified"
      INNER JOIN "date_dim" as "s_return_date_date" on "s_store_returns_unified"."SR_RETURNED_DATE_SK" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "s_return_date_date"."D_WEEK_SEQ" in $1

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
       true  as "s_is_returned",
      "s_web_returns_unified"."WR_ITEM_SK" as "s_item_id",
      "s_web_returns_unified"."WR_ORDER_NUMBER" as "s_order_id",
      "s_web_returns_unified"."WR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "web_returns" as "s_web_returns_unified"
      INNER JOIN "date_dim" as "s_return_date_date" on "s_web_returns_unified"."WR_RETURNED_DATE_SK" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "s_return_date_date"."D_WEEK_SEQ" in $1
  ),
  questionable as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_return_quantity" as "s_return_quantity",
      "s_item_items"."I_ITEM_ID" as "s_item_text_id"
  FROM
      "item" as "s_item_items"
      LEFT OUTER JOIN "cheerful" on "s_item_items"."I_ITEM_SK" = "cheerful"."s_item_id"
  WHERE
      "cheerful"."s_is_returned" = True

  GROUP BY
      1,
      2,
      3,
      "cheerful"."s_order_id",
      "s_item_items"."I_ITEM_SK")
  SELECT
      "questionable"."s_item_text_id" as "item_returns_item_code",
      sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) as "store_return_qty",
      ( ( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) / (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) ) / 3 ) * 100 as "store_pct",
      sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) as "catalog_return_qty",
      ( ( sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) / (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) ) / 3 ) * 100 as "catalog_pct",
      sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END) as "web_return_qty",
      ( ( sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END) / (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) ) / 3 ) * 100 as "web_pct",
      (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) / 3 as "three_channel_avg"
  FROM
      "questionable"
  WHERE
      bool_or("questionable"."s_channel" = 'STORE') = True and bool_or("questionable"."s_channel" = 'CATALOG') = True and bool_or("questionable"."s_channel" = 'WEB') = True

  GROUP BY
      1
  ORDER BY
      "item_returns_item_code" asc nulls first,
      "store_return_qty" asc nulls first
  LIMIT (100)]
  [parameters: ([5244, 5257, 5264],)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```
- `trilogy run query83.preql`

  ```text
  Syntax error in query83.preql: HAVING references 'item_returns.has_store', 'item_returns.has_catalog', 'item_returns.has_web', which are not in the SELECT projection (line 22). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --item_returns.has_store, --item_returns.has_catalog, --item_returns.has_web
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query83.preql`

  ```text
  Unexpected error in query83.preql: (_duckdb.BinderException) Binder Error: WHERE clause cannot contain aggregates!

  LINE 71:     bool_or("questionable"."s_channel" = 'STORE') = True and...
               ^
  [SQL:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
       true  as "s_is_returned",
      "s_catalog_returns_unified"."CR_ITEM_SK" as "s_item_id",
      "s_catalog_returns_unified"."CR_ORDER_NUMBER" as "s_order_id",
      "s_catalog_returns_unified"."CR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "catalog_returns" as "s_catalog_returns_unified"
      INNER JOIN "date_dim" as "s_return_date_date" on "s_catalog_returns_unified"."CR_RETURNED_DATE_SK" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "s_return_date_date"."D_WEEK_SEQ" in $1

  UNION ALL
  SELECT
       'STORE'  as "s_channel",
       true  as "s_is_returned",
      "s_store_returns_unified"."SR_ITEM_SK" as "s_item_id",
      "s_store_returns_unified"."SR_TICKET_NUMBER" as "s_order_id",
      "s_store_returns_unified"."SR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "store_returns" as "s_store_returns_unified"
      INNER JOIN "date_dim" as "s_return_date_date" on "s_store_returns_unified"."SR_RETURNED_DATE_SK" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "s_return_date_date"."D_WEEK_SEQ" in $1

  UNION ALL
  SELECT
       'WEB'  as "s_channel",
       true  as "s_is_returned",
      "s_web_returns_unified"."WR_ITEM_SK" as "s_item_id",
      "s_web_returns_unified"."WR_ORDER_NUMBER" as "s_order_id",
      "s_web_returns_unified"."WR_RETURN_QUANTITY" as "s_return_quantity"
  FROM
      "web_returns" as "s_web_returns_unified"
      INNER JOIN "date_dim" as "s_return_date_date" on "s_web_returns_unified"."WR_RETURNED_DATE_SK" = "s_return_date_date"."D_DATE_SK"
  WHERE
      "s_return_date_date"."D_WEEK_SEQ" in $1
  ),
  questionable as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_return_quantity" as "s_return_quantity",
      "s_item_items"."I_ITEM_ID" as "s_item_text_id"
  FROM
      "item" as "s_item_items"
      LEFT OUTER JOIN "cheerful" on "s_item_items"."I_ITEM_SK" = "cheerful"."s_item_id"
  WHERE
      "cheerful"."s_is_returned" = True

  GROUP BY
      1,
      2,
      3,
      "cheerful"."s_order_id",
      "s_item_items"."I_ITEM_SK")
  SELECT
      "questionable"."s_item_text_id" as "item_returns_item_code",
      sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) as "store_return_qty",
      ( ( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) / (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) ) / 3 ) * 100 as "store_pct",
      sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) as "catalog_return_qty",
      ( ( sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) / (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) ) / 3 ) * 100 as "catalog_pct",
      sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END) as "web_return_qty",
      ( ( sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END) / (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) ) / 3 ) * 100 as "web_pct",
      (( sum(CASE WHEN "questionable"."s_channel" = 'STORE' THEN "questionable"."s_return_quantity" ELSE NULL END) + sum(CASE WHEN "questionable"."s_channel" = 'CATALOG' THEN "questionable"."s_return_quantity" ELSE NULL END) ) + sum(CASE WHEN "questionable"."s_channel" = 'WEB' THEN "questionable"."s_return_quantity" ELSE NULL END)) / 3 as "three_channel_avg"
  FROM
      "questionable"
  WHERE
      bool_or("questionable"."s_channel" = 'STORE') = True and bool_or("questionable"."s_channel" = 'CATALOG') = True and bool_or("questionable"."s_channel" = 'WEB') = True

  GROUP BY
      1
  ORDER BY
      "item_returns_item_code" asc nulls first,
      "store_return_qty" asc nulls first
  LIMIT (100)]
  [parameters: ([5244, 5257, 5264],)]
  (Background on this error at: https://sqlalche.me/e/20/f405)
  ```

### `syntax-parse`

- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, then aggregate with rollup for subtotals
with combi…  grouping(combined.channel_label) desc,
    grouping(combined.entity_id) desc,
    combined.channel_label asc,
    combined.entity_id asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 53:19
     |
  53 |     rollup_metrics(combined.channel_label) as channel_type,
     |                   ^---
     |
     = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or JOIN_TYPE
  Location:
  ...nd;  select     rollup_metrics ??? (combined.channel_label) as ch...

  Write stats: received 2847 chars / 2847 bytes; tail: …'ned.channel_label asc,\\n    combined.entity_id asc\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, then aggregate with rollup for subtotals
with combi…  grouping(combined.channel_label) desc,
    grouping(combined.entity_id) desc,
    combined.channel_label asc,
    combined.entity_id asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 46:5
     |
  46 |     by rollup combined.channel_label, combined.entity_id;
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   coalesce(sum(metric), 0)     ??? by rollup combined.channel_lab...

  Write stats: received 2776 chars / 2776 bytes; tail: …'ned.channel_label asc,\\n    combined.entity_id asc\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, then aggregate with rollup for subtotals
with combi…  grouping(combined.channel_label) desc,
    grouping(combined.entity_id) desc,
    combined.channel_label asc,
    combined.entity_id asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 45:56
     |
  45 | def rollup_metrics(metric) -> coalesce(sum(metric), 0) by rollup combined.channel_label, combined.entity_id;
     |                                                        ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup combined.channel_lab...

  Write stats: received 2772 chars / 2772 bytes; tail: …'ned.channel_label asc,\\n    combined.entity_id asc\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, then aggregate with rollup for subtotals
with combi…  grouping(combined.channel_label) desc,
    grouping(combined.entity_id) desc,
    combined.channel_label asc,
    combined.entity_id asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 56:5
     |
  56 |     by rollup combined.channel_label, combined.entity_id
     |     ^---
     |
     = expected metadata, limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...profit), 0) as net_profit     ??? by rollup combined.channel_lab...

  Write stats: received 2722 chars / 2722 bytes; tail: …'ned.channel_label asc,\\n    combined.entity_id asc\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, then aggregate
with combined as union(
    # Arm 1:…  grouping(combined.channel_label) desc,
    grouping(combined.entity_id) desc,
    combined.channel_label asc,
    combined.entity_id asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 46:5
     |
  46 |     by rollup combined.channel_label, combined.entity_id;
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   coalesce(sum(metric), 0)     ??? by rollup combined.channel_lab...

  Write stats: received 2758 chars / 2758 bytes; tail: …'ned.channel_label asc,\\n    combined.entity_id asc\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, then aggregate
with combined as union(
    # Arm 1:…nt,
    @rl(combined.net_profit) as net_profit
order by
    combined.channel_label asc nulls first,
    combined.entity_id asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 46:5
     |
  46 |     by rollup combined.channel_label, combined.entity_id;
     |     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
   coalesce(sum(metric), 0)     ??? by rollup combined.channel_lab...

  Write stats: received 2652 chars / 2652 bytes; tail: …'lls first,\\n    combined.entity_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, roll up for subtotals
with combined as union(
    #…total_returns_amount,
    output.net_profit
order by
    output.channel_type asc nulls first,
    output.entity_identifier asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...aggregates with aggregated as ??? (     select         combined....

  Write stats: received 3793 chars / 3793 bytes; tail: …'rst,\\n    output.entity_identifier asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Entity-level rows with filtered aggregates
with entity_rows as (
    where a…ity_rows.total_returns_amount,
    entity_rows.net_profit
order by
    entity_rows.channel_type asc,
    entity_rows.entity_identifier asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:21
    |
  4 | with entity_rows as (
    |                     ^---
    |
    = expected select_statement or tvf_union_invocation
  Location:
  ...ggregates with entity_rows as ??? (     where all_sales.date.dat...

  Write stats: received 2635 chars / 2635 bytes; tail: …'l_type asc,\\n    entity_rows.entity_identifier asc\\nlimit 100;'.
  ```
- `trilogy file write query29.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Store sales in Sept 1999, mat…ar(catalog_sales.sold_date.date) in (1999, 2000, 2001)) as catalog_sales_qty
order by
  item_code,
  item_desc,
  store_code,
  store_name
limit 100;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:64
     |
  10 | auto catalog_cust_item <- struct(catalog_sales.bill_customer.id, catalog_sales.item.id)
     |                                                                ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...catalog_sales.bill_customer.id ??? , catalog_sales.item.id)   ? y...

  Write stats: received 1436 chars / 1436 bytes; tail: …'tem_code,\\n  item_desc,\\n  store_code,\\n  store_name\\nlimit 100;'.
  ```
- `trilogy file write query29.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

where
  year(store_sales.date.d…sales.item.id
  and year(catalog_sales.sold_date.date) in (1999, 2000, 2001)
order by
  item_code,
  item_desc,
  store_code,
  store_name
limit 100;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...atalog_sales.bill_customer.id ??? inner join store_sales.item.id...

  Write stats: received 900 chars / 900 bytes; tail: …'tem_code,\\n  item_desc,\\n  store_code,\\n  store_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:store_sales select store_sales.customer.id, store_sales.item.id, count(store_sales.line_item) as cnt where year(store_sa…t null and store_sales.customer.id in (select catalog_sales.bill_customer.id where year(catalog_sales.sold_date.date) in (1999, 2000, 2001)) limit 5;`

  ```text
  Syntax error in stdin: Syntax [102]: Using a SQL-style subquery (SELECT/WITH inside parens)? Trilogy does not support subqueries — joins are auto-resolved from dotted paths. To filter on a value that lives on a related dimension, reference its dot-path directly. Example: instead of `where ss.store_id in (select store_id where store.state = 'TN')`, write `where ss.store.state = 'TN'`.
  Location:
  ...nd store_sales.customer.id in ??? (select catalog_sales.bill_cus...
  ```
- `trilogy file write query29.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

# Catalog sales total quantity …eturn_quantity) as store_return_qty,
  sum(cat_qty_map) as catalog_sales_qty
order by
  item_code,
  item_desc,
  store_code,
  store_name
limit 100;`

  ```text
  refused to write 'query29.preql': not syntactically valid Trilogy.

  Parse error:
    --> 16:37
     |
  16 |   and struct(store_sales.customer.id, store_sales.item.id) in cat_qty_map
     |                                     ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...struct(store_sales.customer.id ??? , store_sales.item.id) in cat_...

  Write stats: received 1089 chars / 1089 bytes; tail: …'tem_code,\\n  item_desc,\\n  store_code,\\n  store_name\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:store_sales --import raw.catalog_sales:catalog_sales select store_sales.customer.id, store_sales.item.id, sum(catalog_sa… (1999,2000,2001) inner join store_sales.customer.id = catalog_sales.bill_customer.id inner join store_sales.item.id = catalog_sales.item.id limit 5;`

  ```text
  Syntax error in stdin:  --> 3:371
    |
  3 | select store_sales.customer.id, store_sales.item.id, sum(catalog_sales.quantity) as cat_qty where year(store_sales.date.date)=1999 and month(store_sales.date.date)=9 and store_sales.return_quantity is not null and year(store_sales.return_date.date)=1999 and month(store_sales.return_date.date) between 9 and 12 and year(catalog_sales.sold_date.date) in (1999,2000,2001) inner join store_sales.customer.id = catalog_sales.bill_customer.id inner join store_sales.item.id = catalog_sales.item.id limit 5;
    |                                                                                                                                                                                                                                                                                                                                                                                   ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ate.date) in (1999,2000,2001) ??? inner join store_sales.custome...
  ```
- `trilogy file write query51.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;

# Step 1: Aggregate per-item per-day fo…sale_date = web_daily.sale_date
having
    web_running_max > store_running_max
order by
    item asc nulls first,
    date asc nulls first
limit 100;`

  ```text
  refused to write 'query51.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | rowset store_daily as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
   for store sales in year 2000 ??? rowset store_daily as where st...

  Write stats: received 2131 chars / 2131 bytes; tail: …'   item asc nulls first,\\n    date asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query59.preql --content import raw.store_sales as ss;

# Sum of unit price by store, week, and day-of-week, with null when no sales on tha…ek_seq = next_year.week_seq - 52
order by
  store_name asc nulls first,
  store_code asc nulls first,
  this_year_week_seq asc nulls first
limit 100;`

  ```text
  refused to write 'query59.preql': not syntactically valid Trilogy.

  Parse error:
    --> 58:47
     |
  58 |   and this_year.week_seq = next_year.week_seq - 52
     |                                               ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...week_seq = next_year.week_seq ??? - 52 order by   store_name asc...

  Write stats: received 2561 chars / 2561 bytes; tail: …'nulls first,\\n  this_year_week_seq asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.physica…t,
    sum(ss.ext_list_price) as total_list_price,
    sum(ss.coupon_amt) as total_coupon_amt
order by ss.item.product_name, ss.store.name
limit 100;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [220]: Filter or stray clause after a `join`? A query-scoped join `inner|left|full join <a> = <b>` takes only key equalities — to join on multiple keys, chain `= c` (one equivalence group) or separate distinct groups with `and` (`a = b and c = d`); STACK another `join` clause for a different join type. Note `and` joins KEY EQUALITIES only, not filters. Joins go right after the `select` list (preferred, SQL-like) or before `select`; the order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`. Filter input rows in `where` (before `select`); filter a joined or aggregated RESULT in `having` (select the field, hide it with a leading `--`). Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ion with qualifying_items as  ??? inner join cat_sales_by_item.i...

  Write stats: received 2361 chars / 2361 bytes; tail: …'_amt\\norder by ss.item.product_name, ss.store.name\\nlimit 100;'.
  ```
- `trilogy file write query78.preql --content import raw.store_sales as store_sales;
import raw.web_sales as web_sales;
import raw.catalog_sales as catalog_sale…cost desc,
    store_agg.store_sales_price desc,
    other_qty asc,
    other_wholesale_cost asc,
    other_sales_price asc,
    ratio asc
limit 100;`

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
  ...000, with identified customer ??? rowset store_agg as where stor...

  Write stats: received 3152 chars / 3152 bytes; tail: …'ost asc,\\n    other_sales_price asc,\\n    ratio asc\\nlimit 100;'.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as sales;

def outlet_id -> case 
  when sales.channel = 'STORE' then concat('store', sales.chann…  @rl(sum(sales.net_profit) - sum(coalesce(sales.return_net_loss, 0))) as profit
order by channel_label nulls first, outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:1
    |
  3 | def outlet_id -> case
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...port raw.all_sales as sales;  ??? def outlet_id -> case    when

  Write stats: received 1049 chars / 1049 bytes; tail: …' channel_label nulls first, outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy file write query80.preql -e -c import raw.all_sales as sales;

with base as
where sales.date.date between '2000-08-23'::date and '2000-09-22'::date
 …s,
    @rl(sum(base.net_profit) - sum(base.return_net_loss)) as profit
order by base.channel_label nulls first, base.outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
    --> 25:26
     |
  25 | def rl(metric) -> metric by rollup base.channel_label, base.outlet_id;
     |                          ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...s ;  def rl(metric) -> metric ??? by rollup base.channel_label,

  Write stats: received 1277 chars / 1277 bytes; tail: …'nel_label nulls first, base.outlet_id nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/all_sales:sales select case when sales.channel='STORE' then 'store channel' when sales.channel='CATALOG' then 'catalog channel' when…> 50 and sales.promotion.channel_tv = 'N' and sales.outlet_id is not null and sales.channel_dim_text_id is not null group by ch order by ch limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...annel_dim_text_id is not null ??? group by ch order by ch limit
  ```
- `trilogy run --import raw.date:date select date.date, date.week_seq, date.year from date where date.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date) order by date.date;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...ate, date.week_seq, date.year ??? from date where date.date in (...
  ```

### `syntax-missing-alias`

- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Union sales and returns, then aggregate
with combined as union(
    # Arm 1:…hannel_label, combined.entity_id as net_profit
order by
    combined.channel_label asc nulls first,
    combined.entity_id asc nulls first
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `coalesce(sum(combined.gross_sales), 0) by rollup combined.channel_label as coalesce_sum_combined_gross_sales_0_by_r`
  Location:
  ...sum(combined.gross_sales), 0) ??? by rollup combined.channel_lab...

  Write stats: received 2428 chars / 2428 bytes; tail: …'lls first,\\n    combined.entity_id asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/date:date select min(date.week_seq), max(date.week_seq), date.year where date.year in (2001, 2002) group by date.year;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `min(date.week_seq) as week_seq_min`
  Location:
  ...ate; select min(date.week_seq) ??? , max(date.week_seq), date.yea...
  ```
- `trilogy run --import raw/all_sales:sales select case when sales.channel='STORE' then 'store channel' when sales.channel='CATALOG' then 'catalog channel' when…rrent_price > 50 and sales.promotion.channel_tv = 'N' and sales.outlet_id is not null and sales.channel_dim_text_id is not null order by ch limit 10;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `(sum(sales.net_profit) - sum(coalesce(sales.return_net_loss,0))) by ch as sum_sales_net_profit_sum_coalesce_sales_`
  Location:
  ...ce(sales.return_net_loss,0))) ??? by ch as profit where sales.da...
  ```
- `trilogy file write query83.preql --content import raw.all_sales as s;
import raw.item as item;

# Target week sequences (weeks containing 2000-06-30, 2000-09…= 'CATALOG') = true
    and bool_or(s.channel = 'WEB') = true
order by
    item_code asc nulls first,
    store_return_qty asc nulls first
limit 100;`

  ```text
  refused to write 'query83.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `Three-channel sum for percentage calculation
      -- sum(s.return_quantity) as three_channel_sum_for_percentage_calcula`
  Location:
  ..._return,     -- Three-channel ??? sum for percentage calculation...

  Write stats: received 1650 chars / 1650 bytes; tail: …'nulls first,\\n    store_return_qty asc nulls first\\nlimit 100;'.
  ```

### `cli-misuse`

- `trilogy run query59.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```
- `trilogy explore --import raw.store_sales:ss select ss.item.text_id as item_id, ss.store.name as store_name, ss.store.zip as store_zip, ss.date.year as sale_year;`

  ```text
  No such option: --import Did you mean --expand-imports?
  ```
- `trilogy run query80.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `join-resolution`

- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Could not resolve connections for query with output ['local.final_items<Purpose.PROPERTY>Derivation.FILTER>'] from current model.
  ```

### `file-not-found`

- `trilogy run query64.preql`

  ```text
  Unexpected error in query64.preql: Join key `yr1999.item_id` does not exist
  ```
