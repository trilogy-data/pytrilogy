# Trilogy failure analysis — 20260625-120155

- Run `20260625-120155` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 351 | failed: 63 (18%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 42 | 67% |
| `syntax-parse` | 10 | 16% |
| `syntax-missing-alias` | 4 | 6% |
| `file-not-found` | 3 | 5% |
| `cli-misuse` | 3 | 5% |
| `undefined-concept` | 1 | 2% |

## Detail

### `other`

- `trilogy run query02.preql duckdb`

  ```text
  Unexpected error in query02.preql: 'Function' object has no attribute 'reference'
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.date.week_seq, sum(ws_ext_sales_price ? all_sales.date.day_of_week = 0) as sun, lead(sum(ws_ext…es_price ? all_sales.date.day_of_week = 0), 53) over (order by all_sales.date.week_seq asc) as next_sun where all_sales.channel in ('WEB','CATALOG');`

  ```text
  Syntax error in stdin: Undefined concept: ws_ext_sales_price. Suggestions: ['all_sales.ext_sales_price', 'all_sales.sales_price', 'all_sales.list_price']
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: SELECT output 'local.total_returns' is defined by an expression that references 'local.total_returns' itself (line 35). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as total_returns_out`).
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(local.channel_type)` (line 60), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_type) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(local.channel_type)` (line 58), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_type) as g order by g desc`.
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: Undefined concept: ext_sales_price. Suggestions: ['all_sales.ext_sales_price', 'all_sales.list_price', 'all_sales.sales_price']
  ```
- `trilogy run query05.preql`

  ```text
  Syntax error in query05.preql: ORDER BY contains aggregate `grouping(local.channel_type)` (line 51), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --grouping(local.channel_type) as g order by g desc`.
  ```
- `trilogy file read query05.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: store_2001.cust_id. Suggestions: ['store_2001.store_rev.cust_id', 'store_rev.cust_id', 'web_rev.cust_id', 'store_2002.store_rev.cust_id', 'web_2001.web_rev.cust_id', 'web_2002.web_rev.cust_id']
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: All case expressions must have the same output datatype, got {<DataType.FLOAT: 'float'>, <DataType.NUMERIC: 'numeric'>} from {'cast(0,FLOAT)': <DataType.FLOAT: 'float'>, 'divide(parenthetical(subtract(ref:local.store_rev_2002_cust,ref:local.store_rev_2001_cust)),cast(ref:local.store_rev_2001_cust,FLOAT))': TraitDataType(type=<DataType.NUMERIC: 'numeric'>, traits=['usd'])}
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 3 disconnected subgraphs: {c.first_name, c.id, c.last_name, c.preferred_cust_flag, local.billing_customer_code}; {local.store_growth, local.store_rev_2001_cust}; {local.web_growth, local.web_rev_2001_cust}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query11.preql`

  ```text
  Syntax error in query11.preql: Undefined concept: sr.sr.cust_id. Suggestions: ['sr.cust_id', 'wr.cust_id', '_sr_cust_id']
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {sr01.sr.rev, sr02.sr.rev, wr01.wr.rev, wr02.wr.cust_id, wr02.wr.rev}
  ```
- `trilogy run query11.preql`

  ```text
  Resolution error in query11.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {c.first_name, c.last_name, c.preferred_cust_flag, local.billing_customer_code}; {sr01.sr.rev, sr02.sr.rev, wr01.wr.rev, wr02.wr.rev}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read query11.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 117). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Recursion error building concept local.channel with grain Grain<Abstract> and lineage UnionSelectLineage(selects=[SelectLineage(selection=[ref:store_nov2001.channel, ref:store_nov2001.brand_id, ref:store_nov2001.class_id, ref:store_nov2001.category_id, ref:store_nov2001.total_sales, ref:store_nov2001.num_sales], hidden_components=set(), local_concepts={'store_nov2001.channel': store_nov2001.channel@Grain<store_nov2001.brand_id,store_nov2001.category_id,store_nov2001.class_id>, 'store_nov2001.brand_id': store_nov2001.brand_id@Grain<store_nov2001.brand_id,store_nov2001.category_id,store_nov2001.class_id>, 'store_nov2001.class_id': store_nov2001.class_id@Grain<store_nov2001.brand_id,store_nov2001.category_id,store_nov2001.class_id>, 'store_nov2001.category_id': store_nov2001.category_id@Grain<store_nov2001.brand_id,store_nov2001.category_id,store_nov2001.class_id>, 'store_nov2001.total_sales': store_nov2001.total_sales@Grain<store_nov2001.brand_id,store_nov2001.category_id,store_nov2001.class_id>, 'store_nov2001.num_sales': store_nov2001.num_sales@Grain<store_nov2001.brand_id,store_nov2001.category_id,store_nov2001.class_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'store_nov2001.brand_id', 'store_nov2001.class_id', 'store_nov2001.category_id'}, where_clause=None, component_order=['store_nov2001.brand_id', 'store_nov2001.class_id', 'store_nov2001.category_id']), where_clause=None, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:catalog_nov2001.channel, ref:catalog_nov2001.brand_id, ref:catalog_nov2001.class_id, ref:catalog_nov2001.category_id, ref:catalog_nov2001.total_sales, ref:catalog_nov2001.num_sales], hidden_components=set(), local_concepts={'catalog_nov2001.channel': catalog_nov2001.channel@Grain<catalog_nov2001.brand_id,catalog_nov2001.category_id,catalog_nov2001.class_id>, 'catalog_nov2001.brand_id': catalog_nov2001.brand_id@Grain<catalog_nov2001.brand_id,catalog_nov2001.category_id,catalog_nov2001.class_id>, 'catalog_nov2001.class_id': catalog_nov2001.class_id@Grain<catalog_nov2001.brand_id,catalog_nov2001.category_id,catalog_nov2001.class_id>, 'catalog_nov2001.category_id': catalog_nov2001.category_id@Grain<catalog_nov2001.brand_id,catalog_nov2001.category_id,catalog_nov2001.class_id>, 'catalog_nov2001.total_sales': catalog_nov2001.total_sales@Grain<catalog_nov2001.brand_id,catalog_nov2001.category_id,catalog_nov2001.class_id>, 'catalog_nov2001.num_sales': catalog_nov2001.num_sales@Grain<catalog_nov2001.brand_id,catalog_nov2001.category_id,catalog_nov2001.class_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'catalog_nov2001.category_id', 'catalog_nov2001.brand_id', 'catalog_nov2001.class_id'}, where_clause=None, component_order=['catalog_nov2001.brand_id', 'catalog_nov2001.class_id', 'catalog_nov2001.category_id']), where_clause=None, having_clause=None, scoped_joins=[]), SelectLineage(selection=[ref:web_nov2001.channel, ref:web_nov2001.brand_id, ref:web_nov2001.class_id, ref:web_nov2001.category_id, ref:web_nov2001.total_sales, ref:web_nov2001.num_sales], hidden_components=set(), local_concepts={'web_nov2001.channel': web_nov2001.channel@Grain<web_nov2001.brand_id,web_nov2001.category_id,web_nov2001.class_id>, 'web_nov2001.brand_id': web_nov2001.brand_id@Grain<web_nov2001.brand_id,web_nov2001.category_id,web_nov2001.class_id>, 'web_nov2001.class_id': web_nov2001.class_id@Grain<web_nov2001.brand_id,web_nov2001.category_id,web_nov2001.class_id>, 'web_nov2001.category_id': web_nov2001.category_id@Grain<web_nov2001.brand_id,web_nov2001.category_id,web_nov2001.class_id>, 'web_nov2001.total_sales': web_nov2001.total_sales@Grain<web_nov2001.brand_id,web_nov2001.category_id,web_nov2001.class_id>, 'web_nov2001.num_sales': web_nov2001.num_sales@Grain<web_nov2001.brand_id,web_nov2001.category_id,web_nov2001.class_id>}, order_by=None, limit=None, meta=Metadata(description=None, line_number=None, column=None, end_line=None, end_column=None, concept_source=<ConceptSource.MANUAL: 'manual'>, hidden=False), grain=Grain(components={'web_nov2001.category_id', 'web_nov2001.brand_id', 'web_nov2001.class_id'}, where_clause=None, component_order=['web_nov2001.brand_id', 'web_nov2001.class_id', 'web_nov2001.category_id']), where_clause=None, having_clause=None, scoped_joins=[])], align=AlignClause(items=[AlignItem(alias='channel', concepts=[ref:store_nov2001.channel, ref:catalog_nov2001.channel, ref:web_nov2001.channel], namespace='local', hidden=False), AlignItem(alias='brand_id', concepts=[ref:store_nov2001.brand_id, ref:catalog_nov2001.brand_id, ref:web_nov2001.brand_id], namespace='local', hidden=False), AlignItem(alias='class_id', concepts=[ref:store_nov2001.class_id, ref:catalog_nov2001.class_id, ref:web_nov2001.class_id], namespace='local', hidden=False), AlignItem(alias='category_id', concepts=[ref:store_nov2001.category_id, ref:catalog_nov2001.category_id, ref:web_nov2001.category_id], namespace='local', hidden=False), AlignItem(alias='total_sales', concepts=[ref:store_nov2001.total_sales, ref:catalog_nov2001.total_sales, ref:web_nov2001.total_sales], namespace='local', hidden=False), AlignItem(alias='num_sales', concepts=[ref:store_nov2001.num_sales, ref:catalog_nov2001.num_sales, ref:web_nov2001.num_sales], namespace='local', hidden=False)]), namespace='local', hidden_components=set(), order_by=None, limit=None, where_clause=None, having_clause=None, derive=None). This is likely due to a circular reference.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 107). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 4 disconnected subgraphs: {combined.brand_id, combined.category_id, combined.channel, combined.class_id}; {local.cs_ct, local.cs_ttl, local.ss_ct, local.ss_ttl, local.ws_ct, local.ws_ttl}; {local.num_sales}; {local.total_sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'ss.quantity', 'ss.list_price', 'ss.date.year', 'cs.quantity', 'cs.list_price', 'cs.sold_date.year', 'ws.quantity', 'ws.list_price', 'ws.date.year', 'ss.line_item', 'cs.line_item', 'ws.line_item', which are not in the SELECT projection (line 97). To filter output rows, add them to SELECT — prefix each with `--` so they stay out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --ss.quantity, --ss.list_price, --ss.date.year, --cs.quantity, --cs.list_price, --cs.sold_date.year, --ws.quantity, --ws.list_price, --ws.date.year, --ss.line_item, --cs.line_item, --ws.line_item
  Or move them to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'avg_sale.overall_avg', which is not in the SELECT projection (line 117). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --avg_sale.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "s_channel",
      "s_catalog_sales_unified"."CS_SOLD_DATE_SK" as "s_date_id",
      "s_catalog_sales_unified"."CS_ITEM_SK" as "s_item_id",
      "s_catalog_sales_unified"."CS_LIST_PRICE" as "s_list_price",
      "s_catalog_sales_unified"."CS_ORDER_NUMBER" as "s_order_id",
      "s_catalog_sales_unified"."CS_QUANTITY" as "s_quantity",
       1  as "s_row_one"
  FROM
      "catalog_sales" as "s_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "s_channel",
      "s_store_sales_unified"."SS_SOLD_DATE_SK" as "s_date_id",
      "s_store_sales_unified"."SS_ITEM_SK" as "s_item_id",
      "s_store_sales_unified"."SS_LIST_PRICE" as "s_list_price",
      "s_store_sales_unified"."SS_TICKET_NUMBER" as "s_order_id",
      "s_store_sales_unified"."SS_QUANTITY" as "s_quantity",
       1  as "s_row_one"
  FROM
      "store_sales" as "s_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "s_channel",
      "s_web_sales_unified"."WS_SOLD_DATE_SK" as "s_date_id",
      "s_web_sales_unified"."WS_ITEM_SK" as "s_item_id",
      "s_web_sales_unified"."WS_LIST_PRICE" as "s_list_price",
      "s_web_sales_unified"."WS_ORDER_NUMBER" as "s_order_id",
      "s_web_sales_unified"."WS_QUANTITY" as "s_quantity",
       1  as "s_row_one"
  FROM
      "web_sales" as "s_web_sales_unified"),
  macho as (
  SELECT
      sum("cheerful"."s_quantity" * "cheerful"."s_list_price") / count("cheerful"."s_row_one") as "_avg_sale_overall_avg"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
  WHERE
      "s_date_date"."D_YEAR" BETWEEN 1999 AND 2001
  ),
  questionable as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "cheerful"."s_item_id" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" BETWEEN 1999 AND 2001

  GROUP BY
      1,
      2,
      3,
      4),
  friendly as (
  SELECT
      "macho"."_avg_sale_overall_avg" as "avg_sale_overall_avg"
  FROM
      "macho"),
  abundant as (
  SELECT
      "questionable"."s_item_brand_id" as "_qualifying_combos_brand_id",
      "questionable"."s_item_category_id" as "_qualifying_combos_category_id",
      "questionable"."s_item_class_id" as "_qualifying_combos_class_id"
  FROM
      "questionable"
  GROUP BY
      1,
      2,
      3
  HAVING
      count(distinct "questionable"."s_channel") = 3
  ),
  uneven as (
  SELECT
      "abundant"."_qualifying_combos_brand_id" as "_qualifying_combos_brand_id",
      "abundant"."_qualifying_combos_category_id" as "_qualifying_combos_category_id",
      "abundant"."_qualifying_combos_class_id" as "_qualifying_combos_class_id"
  FROM
      "abundant"),
  yummy as (
  SELECT
      "uneven"."_qualifying_combos_brand_id" as "qualifying_combos_brand_id",
      "uneven"."_qualifying_combos_category_id" as "qualifying_combos_category_id",
      "uneven"."_qualifying_combos_class_id" as "qualifying_combos_class_id"
  FROM
      "uneven"),
  concerned as (
  SELECT
      "yummy"."qualifying_combos_class_id" as "qualifying_combos_class_id"
  FROM
      "yummy"
  GROUP BY
      1),
  vacuous as (
  SELECT
      "yummy"."qualifying_combos_category_id" as "qualifying_combos_category_id"
  FROM
      "yummy"
  GROUP BY
      1),
  juicy as (
  SELECT
      "yummy"."qualifying_combos_brand_id" as "qualifying_combos_brand_id"
  FROM
      "yummy"
  GROUP BY
      1),
  young as (
  SELECT
      "cheerful"."s_channel" as "s_channel",
      "cheerful"."s_list_price" as "s_list_price",
      "cheerful"."s_quantity" as "s_quantity",
      "cheerful"."s_row_one" as "s_row_one",
      "s_item_items"."I_BRAND_ID" as "s_item_brand_id",
      "s_item_items"."I_CATEGORY_ID" as "s_item_category_id",
      "s_item_items"."I_CLASS_ID" as "s_item_class_id"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "s_date_date" on "cheerful"."s_date_id" = "s_date_date"."D_DATE_SK"
      INNER JOIN "item" as "s_item_items" on "cheerful"."s_item_id" = "s_item_items"."I_ITEM_SK"
  WHERE
      "s_date_date"."D_YEAR" = 2001 and "s_date_date"."D_MOY" = 11 and "s_item_items"."I_BRAND_ID" in (select juicy."qualifying_combos_brand_id" from juicy where juicy."qualifying_combos_brand_id" is not null) and "s_item_items"."I_CLASS_ID" in (select concerned."qualifying_combos_class_id" from concerned where concerned."qualifying_combos_class_id" is not null) and "s_item_items"."I_CATEGORY_ID" in (select vacuous."qualifying_combos_category_id" from vacuous where vacuous."qualifying_combos_category_id" is not null)

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      "cheerful"."s_item_id",
      "cheerful"."s_order_id"),
  sparkling as (
  SELECT
      "young"."s_channel" as "_channel_data_channel",
      "young"."s_item_brand_id" as "_channel_data_brand_id",
      "young"."s_item_category_id" as "_channel_data_category_id",
      "young"."s_item_class_id" as "_channel_data_class_id",
      count("young"."s_row_one") as "_channel_data_num_sales",
      sum("young"."s_quantity" * "young"."s_list_price") as "_channel_data_total_sales"
  FROM
      "young"
  GROUP BY
      1,
      2,
      3,
      4),
  abhorrent as (
  SELECT
      "sparkling"."_channel_data_brand_id" as "channel_data_brand_id",
      "sparkling"."_channel_data_category_id" as "channel_data_category_id",
      "sparkling"."_channel_data_channel" as "channel_data_channel",
      "sparkling"."_channel_data_class_id" as "channel_data_class_id",
      "sparkling"."_channel_data_num_sales" as "channel_data_num_sales",
      "sparkling"."_channel_data_total_sales" as "channel_data_total_sales"
  FROM
      "sparkling"),
  sweltering as (
  SELECT
      "abhorrent"."channel_data_brand_id" as "channel_data_brand_id",
      "abhorrent"."channel_data_category_id" as "channel_data_category_id",
      "abhorrent"."channel_data_channel" as "channel_data_channel",
      "abhorrent"."channel_data_class_id" as "channel_data_class_id",
      "abhorrent"."channel_data_num_sales" as "channel_data_num_sales",
      "abhorrent"."channel_data_total_sales" as "channel_data_total_sales"
  FROM
      "abhorrent"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6),
  late as (
  SELECT
      "sweltering"."channel_data_brand_id" as "channel_data_brand_id",
      "sweltering"."channel_data_category_id" as "channel_data_category_id",
      "sweltering"."channel_data_channel" as "channel_data_channel",
      "sweltering"."channel_data_class_id" as "channel_data_class_id",
      sum("sweltering"."channel_data_num_sales") as "num_sales",
      sum("sweltering"."channel_data_total_sales") as "total_sales"
  FROM
      "sweltering"
  GROUP BY
      ROLLUP (3, 1, 4, 2))
  SELECT
      "late"."channel_data_channel" as "channel_data_channel",
      "late"."channel_data_brand_id" as "channel_data_brand_id",
      "late"."channel_data_class_id" as "channel_data_class_id",
      "late"."channel_data_category_id" as "channel_data_category_id",
      "late"."total_sales" as "total_sales",
      "late"."num_sales" as "num_sales"
  FROM
      "friendly"
      LEFT OUTER JOIN "late" on 1=1
  WHERE
      INVALID_REFERENCE_BUG * INVALID_REFERENCE_BUG > "friendly"."avg_sale_overall_avg"

  ORDER BY
      "late"."channel_data_channel" asc nulls first,
      "late"."channel_data_brand_id" asc nulls first,
      "late"."channel_data_class_id" asc nulls first,
      "late"."channel_data_category_id" asc nulls first
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 4 disconnected subgraphs: {channel_data.brand_id, channel_data.category_id, channel_data.channel, channel_data.class_id}; {local._virt_agg_count_4117327293185120, local._virt_agg_sum_1265922547819501}; {local.num_sales}; {local.total_sales}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: HAVING references 'local.overall_avg', which is not in the SELECT projection (line 20). To filter output rows, add it to SELECT — prefix each with `--` so it stays out of the output rows, keeping your HAVING as-is:
      select <your existing columns>, --local.overall_avg
  Or move it to WHERE to filter before aggregation; for an aggregate condition on a non-output grain, write `agg(x) by grain` inline in WHERE.
  ```
- `trilogy file read query14.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/date.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: year1999.item_id. Suggestions: ['year1999.agg_rows.item_id', 'agg_rows.item_id', 'year2000.agg_rows.item_id', 'year1999.store_zip', 'year1999.sale_zip']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 13 undefined concept references; fix all before re-running:
    - year1999.product_name (line 104, col 3, in SELECT); did you mean: year1999.agg_rows.product_name, ss.item.product_name, cs.item.product_name, cr.sales.item.product_name, cr.item.product_name, pr.item.product_name?
    - year1999.store_name (line 105, col 3, in SELECT); did you mean: year1999.agg_rows.store_name, agg_rows.store_name, year2000.agg_rows.store_name, year1999.sale_street_name, year1999.cust_street_name, year1999.store_zip?
    - year1999.store_zip (line 106, col 3, in SELECT); did you mean: year1999.agg_rows.store_zip, agg_rows.store_zip, year2000.agg_rows.store_zip, year1999.sale_zip, year1999.cust_zip, year1999.store_name?
    - year1999.sale_street_number (line 107, col 3, in SELECT); did you mean: year1999.agg_rows.sale_street_number, agg_rows.sale_street_number, year2000.agg_rows.sale_street_number, year1999.sale_street_name, year1999.cust_street_number?
    - year1999.sale_street_name (line 108, col 3, in SELECT); did you mean: year1999.agg_rows.sale_street_name, agg_rows.sale_street_name, year2000.agg_rows.sale_street_name, year1999.sale_street_number, year1999.cust_street_name?
    - year1999.sale_city (line 109, col 3, in SELECT); did you mean: year1999.agg_rows.sale_city, agg_rows.sale_city, year2000.agg_rows.sale_city, year1999.sale_zip, year1999.cust_city?
    - year1999.sale_zip (line 110, col 3, in SELECT); did you mean: year1999.agg_rows.sale_zip, agg_rows.sale_zip, year2000.agg_rows.sale_zip, year1999.store_zip, year1999.sale_city, year1999.cust_zip?
    - year1999.cust_street_number (line 111, col 3, in SELECT); did you mean: year1999.agg_rows.cust_street_number, agg_rows.cust_street_number, year2000.agg_rows.cust_street_number, year1999.cust_street_name, year1999.sale_street_number?
    - year1999.cust_street_name (line 112, col 3, in SELECT); did you mean: year1999.agg_rows.cust_street_name, agg_rows.cust_street_name, year2000.agg_rows.cust_street_name, year1999.cust_street_number, year1999.sale_street_name?
    - year1999.cust_city (line 113, col 3, in SELECT); did you mean: year1999.agg_rows.cust_city, agg_rows.cust_city, year2000.agg_rows.cust_city, year1999.cust_zip, year1999.sale_city?
    - year1999.cust_zip (line 114, col 3, in SELECT); did you mean: year1999.agg_rows.cust_zip, agg_rows.cust_zip, year2000.agg_rows.cust_zip, year1999.store_zip, year1999.cust_city, year1999.sale_zip?
    - year1999.product_name (line 127, col 3, in ORDER BY); did you mean: year1999.agg_rows.product_name, ss.item.product_name, cs.item.product_name, cr.sales.item.product_name, cr.item.product_name, pr.item.product_name?
    - year1999.store_name (line 128, col 3, in ORDER BY); did you mean: year1999.agg_rows.store_name, agg_rows.store_name, year2000.agg_rows.store_name, year1999.sale_street_name, year1999.cust_street_name, year1999.store_zip?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: 5 undefined concept references; fix all before re-running:
    - year2000.agg_rows.sale_lines (line 102, col 7, in WHERE); did you mean: agg_rows.sale_lines, year1999.agg_rows.sale_lines, year2000.agg_rows.sale_zip, year2000.agg_rows.sale_city?
    - year1999.agg_rows.sale_lines (line 102, col 39, in WHERE); did you mean: agg_rows.sale_lines, year2000.agg_rows.sale_lines, year1999.agg_rows.sale_zip, year1999.agg_rows.sale_city?
    - year2000.agg_rows.sale_lines (line 129, col 3, in ORDER BY); did you mean: agg_rows.sale_lines, year1999.agg_rows.sale_lines, year2000.agg_rows.sale_zip, year2000.agg_rows.sale_city?
    - year1999.agg_rows.wholesale_sum (line 130, col 3, in ORDER BY); did you mean: agg_rows.wholesale_sum, year2000.agg_rows.wholesale_sum, year1999.agg_rows.sale_lines?
    - year2000.agg_rows.wholesale_sum (line 131, col 3, in ORDER BY); did you mean: agg_rows.wholesale_sum, year1999.agg_rows.wholesale_sum, year2000.agg_rows.sale_lines?
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: y1999.agg_rows.sale_year. Suggestions: ['agg_rows.sale_year', 'y1999.agg_rows.sale_city', 'y1999.agg_rows.first_sales_year', 'y1999.agg_rows.sale_lines']
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {i.id, local._agg_rows_item_id, local._agg_rows_product_name, local.item_color_ok, local.item_has_store_return, local.item_price_ok}; {local._agg_rows_coupon_sum, local._agg_rows_cust_city, local._agg_rows_cust_street_name, local._agg_rows_cust_street_number, local._agg_rows_cust_zip, local._agg_rows_first_sales_year, local._agg_rows_first_ship_year, local._agg_rows_list_price_sum, local._agg_rows_sale_city, local._agg_rows_sale_lines, local._agg_rows_sale_street_name, local._agg_rows_sale_street_number, local._agg_rows_sale_year, local._agg_rows_sale_zip, local._agg_rows_store_name, local._agg_rows_store_zip, local._agg_rows_wholesale_sum, local.marital_diff}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {i.id, local.item_color_ok, local.item_has_store_return, local.item_id, local.item_price_ok, local.product_name}; {local.coupon_sum, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_ship_year, local.list_price_sum, local.marital_diff, local.sale_city, local.sale_lines, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_sum}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query64.preql`

  ```text
  Resolution error in query64.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {i.color, i.current_price, i.id, local.item_id, local.product_name}; {local.coupon_sum, local.cust_city, local.cust_street_name, local.cust_street_number, local.cust_zip, local.first_sales_year, local.first_ship_year, local.list_price_sum, local.sale_city, local.sale_lines, local.sale_street_name, local.sale_street_number, local.sale_year, local.sale_zip, local.store_name, local.store_zip, local.wholesale_sum, ss.customer.demographics.marital_status, ss.customer_demographic.marital_status}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: item.category. Suggestions: ['sales.item.category', 'sales.item.category_id']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: Undefined concept: deduped.yr. Suggestions: ['deduped.per_line.yr', 'per_line.yr', 'deduped.per_line.mfr', 'deduped.per_line.cat']
  ```
- `trilogy run query75.preql`

  ```text
  Syntax error in query75.preql: 2 undefined concept references; fix all before re-running:
    - yr2002.total_amt (line 66, in SELECT); did you mean: annual.total_amt, yr2001.total_amt, yr2002.cat, _yr2002_cat?
    - yr2001.total_amt (line 66, in SELECT); did you mean: annual.total_amt, yr2002.total_amt, yr2001.cat, _yr2001_cat?
  ```
- `trilogy run query75.preql`

  ```text
  Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {yr2001.amt_2001, yr2001.brand, yr2001.cat, yr2001.class, yr2001.mfr, yr2001.qty_2001}; {yr2002.amt_2002, yr2002.brand, yr2002.cat, yr2002.class, yr2002.mfr, yr2002.qty_2002}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy agent-info syntax example rollup-syntax`

  ```text
  Unknown syntax example: 'rollup-syntax'

  Available Trilogy syntax examples — print one with `trilogy agent-info syntax example <name>`:

  - `query-structure` — the clause order of a query (`where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`) and how to define a rowset (a NAME then a select); joins sit right after the select list — filter a joined or aggregated RESULT in `having`, input rows in `where`
  - `filtered-aggregate` — `sum(x ? cond)` / `count(x ? cond)` aggregate just the matching rows; to COUNT ROWS count the unique grain/row key, not a non-unique sub-key; `by <
  …
  sum — the `distinct`/UNION substitute
  - `rank-over-rollup` — rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by level, parent ...)` — not separate ranks per level
  - `staged-membership` — compute a membership set in a `rowset` (keys meeting a count/HAVING), then filter the main query with `<key> in <rowset>.<col>`
  - `correlated-exists-via-grouped-counts` — translate `EXISTS other` / `NOT EXISTS other matching` over the same model into two `count(...) by <grain>` compared in `where` (`> 1` = another exists, `= 1` = no other matches) — don't filter on a boolean-of-aggregate
  ```
- `trilogy file read query80.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write query02.preql`

  ```text
  refused to write 'query02.preql': not syntactically valid Trilogy.

  Parse error:
    --> 49:54
     |
  49 | inner join weekly_totals.week_seq = shifted.week_seq + 53
     |                                                      ^---
     |
     = expected limit, order_by, where, having, or JOIN_TYPE
  Location:
  ...s.week_seq = shifted.week_seq ??? + 53
   where weekly_totals.week...

  Write stats: received 2409 chars / 2409 bytes; tail: …'_in_2001\r\\norder by weekly_totals.week_seq asc nulls first;\r\\n'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filtered metrics

# Gross sales (ext_sales_price) for rows with sale in peri… grouping(effective_channel_label) asc, 
    effective_channel_label asc,
    grouping(combined_entity_id) asc,
    combined_entity_id asc
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
    --> 20:29
     |
  20 |   coalesce(metric_sales, 0) as total_gross_sales,
     |                             ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
      coalesce(metric_sales, 0) ??? as total_gross_sales,   coales...

  Write stats: received 3701 chars / 3701 bytes; tail: …'mbined_entity_id) asc,\\n    combined_entity_id asc\\nlimit 100;'.
  ```
- `trilogy file write query05.preql --content import raw.all_sales as all_sales;

# Filtered metrics use inline conditions with ?
# The WHERE just ensures we ha…oalesce(returns_by_entity, 0) as total_returns,
    coalesce(sales_profit_by_entity, 0) - coalesce(return_loss_by_entity, 0) as net_profit
limit 100;`

  ```text
  refused to write 'query05.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [104]: Definition or statement after WHERE/SELECT? Concept definitions (`auto`/`property`/`key`/`metric`/`rowset`), `def`, `datasource`, and `import` are top-level statements and must appear BEFORE the `where`/`select` block — they cannot sit inside a query. Move this statement above your `where`, and make sure each statement ends with `;`. Example: put `auto x <- sum(sales.amount) by store.id;` above `where ... select ...`.
  Location:
  ...ss sales (only for sale rows) ??? auto gross_sales_by_entity <-

  Write stats: received 2864 chars / 2864 bytes; tail: …' coalesce(return_loss_by_entity, 0) as net_profit\\nlimit 100;'.
  ```
- `trilogy run --import raw.all_sales:as select as.channel, count(*) as cnt, count(1) limit 5;`

  ```text
  Syntax error in stdin:  --> 2:26
    |
  2 | select as.channel, count(*) as cnt, count(1) limit 5;
    |                          ^---
    |
    = expected access_chain
  Location:
   as; select as.channel, count( ??? *) as cnt, count(1) limit 5;
  ```
- `trilogy file write query14.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;
import raw.item as item;…l asc nulls first,
    combined.brand_id asc nulls first,
    combined.class_id asc nulls first,
    combined.category_id asc nulls first
limit 100;
`

  ```text
  refused to write 'query14.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...year between 1999 and 2001) ) ??? by *;  # Combine the three cha...

  Write stats: received 5002 chars / 5002 bytes; tail: …' first,\\n    combined.category_id asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run - --import raw.catalog_store_returns:csr duckdb`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   ('2001Q1','2001Q2','2001Q3') ??? group by csr.store_return_date...
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: items…total_sales
order by feb2000.last_name asc nulls first,
         feb2000.first_name asc nulls first,
         total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:117
     |
  10 | auto item_date_count_by_desc30 <- count_distinct(concat(ss.item.id::string, '-', ss.date.date::string)) by substring(ss.item.desc, 1, 30) ? ss.date.year between 2000 and 2003;
     |                                                                                                                     ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...te.date::string)) by substring ??? (ss.item.desc, 1, 30) ? ss.dat...

  Write stats: received 2377 chars / 2377 bytes; tail: …'ulls first,\\n         total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: items…total_sales
order by feb2000.last_name asc nulls first,
         feb2000.first_name asc nulls first,
         total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:154
     |
  10 | auto item_date_count_by_desc30 <- count_distinct(concat(ss.item.id::string, '-', ss.date.date::string) ? ss.date.year between 2000 and 2003) by substring(ss.item.desc, 1, 30);
     |                                                                                                                                                          ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
  ...en 2000 and 2003) by substring ??? (ss.item.desc, 1, 30);  auto f...

  Write stats: received 2379 chars / 2379 bytes; tail: …'ulls first,\\n         total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query23.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent items: items…total_sales
order by feb2000.last_name asc nulls first,
         feb2000.first_name asc nulls first,
         total_sales asc nulls first
limit 100;
`

  ```text
  refused to write 'query23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:6
     |
  27 |     (import raw.catalog_sales as cs_arm
     |      ^---
     |
     = expected select_statement
  Location:
    with feb2000 as union(     ( ??? import raw.catalog_sales as cs...

  Write stats: received 2266 chars / 2266 bytes; tail: …'ulls first,\\n         total_sales asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write query80.preql --content import raw.all_sales as s;

# Channel label based on the channel enum
auto channel_label <- 
  case 
    when s.ch…esce(sum(s.return_net_loss), 0) by rollup channel_label, outlet_id as profit
order by
  channel_label nulls first,
  outlet_id nulls first
limit 100;`

  ```text
  refused to write 'query80.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [212]: A `by <grain>` clause must attach directly to an aggregate, not to an expression that wraps one (e.g. `coalesce(...)`, `round(...)`, arithmetic). Move the grain inside, next to the aggregate — write `coalesce(sum(x) by store.id, 0)` — or compute the grouped aggregate first and wrap it: `auto m <- sum(x) by store.id;` then `coalesce(m, 0)`.
  Location:
  ...) -> coalesce(sum(metric), 0) ??? by rollup channel_label, outle...

  Write stats: received 1294 chars / 1294 bytes; tail: …'hannel_label nulls first,\\n  outlet_id nulls first\\nlimit 100;'.
  ```

### `syntax-missing-alias`

- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.date.week_seq where all_sales.date.year in (2001,2002) and all_sales.channel in ('WEB','CATALOG') order by all_sales.date.week_seq asc;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct all_sales.date.week_seq as distinct_all_sales_date_week_seq`
  Location:
  ...as all_sales; select distinct ??? all_sales.date.week_seq where
  ```
- `trilogy run --import raw.all_sales:all_sales select distinct all_sales.date.year, all_sales.date.week_seq where all_sales.date.week_seq = 5270 and all_sales.channel in ('WEB','CATALOG');`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `distinct all_sales.date.year as distinct_all_sales_date_year`
  Location:
  ...as all_sales; select distinct ??? all_sales.date.year, all_sales...
  ```
- `trilogy file write query11.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.customer as c;

# For each customer, compute …as wr01,
    sum(ws.ext_list_price - ws.ext_discount_amount ? ws.date.year = 2002 and ws.billing_customer.id = c.id) as wr02
inner join ??
limit 100;`

  ```text
  refused to write 'query11.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `store revenue by year
      sum(ss.ext_list_price - ss.ext_discount_amount ? ss.date.year = 2001 and ss.customer.id = c.id) as store_revenue_by_year_sum_ss_ext_list_pr`
  Location:
  ...erred_cust_flag,     -- store ??? revenue by year     sum(ss.ext...

  Write stats: received 1163 chars / 1163 bytes; tail: …'billing_customer.id = c.id) as wr02\\ninner join ??\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:ss select count(ss.customer.id), sum(ss.ext_sales_price), min(ss.date.month_seq), max(ss.date.month_seq) where ss.date.month_seq between 1188 and 1190;`

  ```text
  Syntax error in stdin: Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `count(ss.customer.id) as id_count`
  Location:
  ...; select count(ss.customer.id) ??? , sum(ss.ext_sales_price), min...
  ```

### `file-not-found`

- `trilogy run query02.preql duckdb`

  ```text
  Unexpected error in query02.preql: Join key `weekly_totals.week_seq` does not exist
  ```
- `trilogy explore query02.preql`

  ```text
  Failed to parse query02.preql: Join key `weekly_totals.week_seq` does not exist
  ```
- `trilogy run query11.preql`

  ```text
  Unexpected error in query11.preql: Join key `store_cust_2001.ss.customer.id` does not exist
  ```

### `cli-misuse`

- `trilogy run - --import raw.store_sales:s select distinct s.date.year, s.date.quarter_name, count(s.quantity) limit 20;`

  ```text
  'select distinct s.date.year, s.date.quarter_name, count(s.quantity) limit 20;' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/store_returns.preql`

  ```text
  Invalid value for 'PATH': File 'raw/store_returns.preql' does not exist.
  ```
- `trilogy run query80.preql --format json`

  ```text
  '--format' is not a valid dialect. The dialect argument comes after the input file and any options.
    Try: trilogy run <input> [<dialect>]
  ```

### `undefined-concept`

- `trilogy explore query11.preql`

  ```text
  Failed to parse query11.preql: (UndefinedConceptException(...), "Undefined concept: store_2001.cust_id. Suggestions: ['store_2001.store_rev.cust_id', 'store_rev.cust_id', 'web_rev.cust_id', 'store_2002.store_rev.cust_id', 'web_2001.web_rev.cust_id', 'web_2002.web_rev.cust_id']")
  ```
