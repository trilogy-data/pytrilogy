# Trilogy failure analysis — 20260628-175514

- Run `20260628-175514` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 217 | failed: 27 (12%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 20 | 74% |
| `syntax-parse` | 5 | 19% |
| `undefined-concept` | 1 | 4% |
| `cli-misuse` | 1 | 4% |

## Detail

### `other`

- `trilogy file read query02.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query14.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run query14.preql`

  ```text
  Syntax error in query14.preql: Ambiguous reference 'nov2001_detail.brand_id': matches ['nov2001_detail.all_sales.item.brand_id', 'nov2001_detail.qualifying_combos.brand_id']. Qualify the full path to disambiguate.
  ```
- `trilogy run query14.preql`

  ```text
  Resolution error in query14.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 51). The requested concepts split into 2 disconnected subgraphs: {all_sales.channel, all_sales.item.id, all_sales.order_id, all_sales.quantity}; {local._level, local.brand_id, local.category_id, local.channel, local.class_id}.
    - `local.brand_id` is disconnected — did you mean `all_sales.item.brand_id`? (connected to the other concepts)
    - `local.category_id` is disconnected — did you mean `all_sales.item.category_id`? (connected to the other concepts)
    - `local.channel` is disconnected — did you mean `all_sales.channel`? (connected to the other concepts)
    - `local.class_id` is disconnected — did you mean `all_sales.item.class_id`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `all_sales.item.brand_id`) instead of importing a second, disconnected copy.
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Filter item with non ref content (ref:all_sales.item.brand_id, ref:all_sales.item.class_id, ref:all_sales.item.category_id) (<class 'trilogy.core.models.core.TupleWrapper'>) not yet supported
  ```
- `trilogy run --import raw.all_sales:all_sales select case when grouping(all_sales.channel)=1 then null else all_sales.channel end as channel, case when groupi…_sales.item.category_id) order by channel asc nulls first, brand_id asc nulls first, class_id asc nulls first, category_id asc nulls first limit 100;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - class_id (line 2, col 692, in ORDER BY); did you mean: all_sales.item.class_id?
    - category_id (line 2, col 718, in ORDER BY); did you mean: all_sales.item.category_id?
  ```
- `trilogy run query14.preql`

  ```text
  Unexpected error in query14.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
       'CATALOG'  as "all_sales_channel",
      "all_sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_catalog_sales_unified"."CS_ITEM_SK" as "all_sales_item_id",
      "all_sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_catalog_sales_unified"."CS_LIST_PRICE" as "all_sales_list_price",
      "all_sales_catalog_sales_unified"."CS_QUANTITY" as "all_sales_quantity"
  FROM
      "catalog_sales" as "all_sales_catalog_sales_unified"
  UNION ALL
  SELECT
       'STORE'  as "all_sales_channel",
      "all_sales_store_sales_unified"."SS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_store_sales_unified"."SS_ITEM_SK" as "all_sales_item_id",
      "all_sales_store_sales_unified"."SS_TICKET_NUMBER" as "all_sales_order_id",
      "all_sales_store_sales_unified"."SS_LIST_PRICE" as "all_sales_list_price",
      "all_sales_store_sales_unified"."SS_QUANTITY" as "all_sales_quantity"
  FROM
      "store_sales" as "all_sales_store_sales_unified"
  UNION ALL
  SELECT
       'WEB'  as "all_sales_channel",
      "all_sales_web_sales_unified"."WS_SOLD_DATE_SK" as "all_sales_date_id",
      "all_sales_web_sales_unified"."WS_ITEM_SK" as "all_sales_item_id",
      "all_sales_web_sales_unified"."WS_ORDER_NUMBER" as "all_sales_order_id",
      "all_sales_web_sales_unified"."WS_LIST_PRICE" as "all_sales_list_price",
      "all_sales_web_sales_unified"."WS_QUANTITY" as "all_sales_quantity"
  FROM
      "web_sales" as "all_sales_web_sales_unified"),
  abundant as (
  SELECT
      "all_sales_item_items"."I_BRAND_ID" as "all_sales_item_brand_id",
      "all_sales_item_items"."I_CATEGORY_ID" as "all_sales_item_category_id",
      "all_sales_item_items"."I_CLASS_ID" as "all_sales_item_class_id",
      CASE WHEN year(cast(cast("all_sales_date_date"."D_DATE" as date) as date)) BETWEEN 1999 AND 2001 THEN "cheerful"."all_sales_channel" ELSE NULL END as "_virt_filter_channel_476671847477763"
  FROM
      "cheerful"
      INNER JOIN "item" as "all_sales_item_items" on "cheerful"."all_sales_item_id" = "all_sales_item_items"."I_ITEM_SK"
      LEFT OUTER JOIN "date_dim" as "all_sales_date_date" on "cheerful"."all_sales_date_id" = "all_sales_date_date"."D_DATE_SK"
  GROUP BY
      1,
      2,
      3,
      4,
      "cheerful"."all_sales_channel"),
  abhorrent as (
  SELECT
      "all_sales_item_items"."I_BRAND_ID" as "all_sales_item_brand_id",
      "all_sales_item_items"."I_CATEGORY_ID" as "all_sales_item_category_id",
      "all_sales_item_items"."I_CLASS_ID" as "all_sales_item_class_id",
      "cheerful"."all_sales_channel" as "all_sales_channel",
      "cheerful"."all_sales_item_id" as "all_sales_item_id"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "all_sales_date_date" on "cheerful"."all_sales_date_id" = "all_sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "all_sales_item_items" on "cheerful"."all_sales_item_id" = "all_sales_item_items"."I_ITEM_SK"
  WHERE
      year(cast(cast("all_sales_date_date"."D_DATE" as date) as date)) = 2001 and "all_sales_date_date"."D_MOY" = 11

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      "all_sales_date_date"."D_MOY",
      cast("all_sales_date_date"."D_DATE" as date)),
  questionable as (
  SELECT
      "all_sales_item_items"."I_BRAND_ID" as "all_sales_item_brand_id",
      "all_sales_item_items"."I_CATEGORY_ID" as "all_sales_item_category_id",
      "all_sales_item_items"."I_CLASS_ID" as "all_sales_item_class_id",
      "cheerful"."all_sales_channel" as "all_sales_channel",
      "cheerful"."all_sales_list_price" as "all_sales_list_price",
      "cheerful"."all_sales_order_id" as "all_sales_order_id",
      "cheerful"."all_sales_quantity" as "all_sales_quantity",
      cast("all_sales_date_date"."D_DATE" as date) as "all_sales_date_date"
  FROM
      "cheerful"
      INNER JOIN "date_dim" as "all_sales_date_date" on "cheerful"."all_sales_date_id" = "all_sales_date_date"."D_DATE_SK"
      INNER JOIN "item" as "all_sales_item_items" on "cheerful"."all_sales_item_id" = "all_sales_item_items"."I_ITEM_SK"
  WHERE
      year(cast(cast("all_sales_date_date"."D_DATE" as date) as date)) = 2001 and "all_sales_date_date"."D_MOY" = 11
  ),
  juicy as (
  SELECT
      "abundant"."all_sales_item_brand_id" as "all_sales_item_brand_id",
      "abundant"."all_sales_item_category_id" as "all_sales_item_category_id",
      "abundant"."all_sales_item_class_id" as "all_sales_item_class_id"
  FROM
      "abundant"
  GROUP BY
      1,
      2,
      3
  HAVING
      count(distinct "abundant"."_virt_filter_channel_476671847477763") = 3
  ),
  sweltering as (
  SELECT
      "abhorrent"."all_sales_channel" as "all_sales_channel",
      "abhorrent"."all_sales_item_brand_id" as "all_sales_item_brand_id",
      "abhorrent"."all_sales_item_category_id" as "all_sales_item_category_id",
      "abhorrent"."all_sales_item_class_id" as "all_sales_item_class_id"
  FROM
      "abhorrent"
      INNER JOIN "juicy" on "abhorrent"."all_sales_item_brand_id" = "juicy"."all_sales_item_brand_id" AND "abhorrent"."all_sales_item_category_id" is not distinct from "juicy"."all_sales_item_category_id" AND "abhorrent"."all_sales_item_class_id" is not distinct from "juicy"."all_sales_item_class_id"
  GROUP BY
      1,
      2,
      3,
      4,
      "abhorrent"."all_sales_item_id"),
  vacuous as (
  SELECT
      "questionable"."all_sales_channel" as "all_sales_channel",
      "questionable"."all_sales_date_date" as "all_sales_date_date",
      "questionable"."all_sales_item_brand_id" as "all_sales_item_brand_id",
      "questionable"."all_sales_item_category_id" as "all_sales_item_category_id",
      "questionable"."all_sales_item_class_id" as "all_sales_item_class_id",
      "questionable"."all_sales_list_price" as "all_sales_list_price",
      "questionable"."all_sales_order_id" as "all_sales_order_id",
      "questionable"."all_sales_quantity" as "all_sales_quantity"
  FROM
      "juicy"
      INNER JOIN "questionable" on "juicy"."all_sales_item_brand_id" = "questionable"."all_sales_item_brand_id" AND "juicy"."all_sales_item_category_id" is not distinct from "questionable"."all_sales_item_category_id" AND "juicy"."all_sales_item_class_id" is not distinct from "questionable"."all_sales_item_class_id"),
  late as (
  SELECT
      "sweltering"."all_sales_channel" as "all_sales_channel",
      "sweltering"."all_sales_item_brand_id" as "all_sales_item_brand_id",
      "sweltering"."all_sales_item_category_id" as "all_sales_item_category_id",
      "sweltering"."all_sales_item_class_id" as "all_sales_item_class_id",
      grouping("sweltering"."all_sales_channel") as "_virt_agg_grouping_9079213539192150",
      grouping("sweltering"."all_sales_item_brand_id") as "_virt_agg_grouping_7230499189600468",
      grouping("sweltering"."all_sales_item_category_id") as "_virt_agg_grouping_8745896301782811",
      grouping("sweltering"."all_sales_item_class_id") as "_virt_agg_grouping_5728540759741229"
  FROM
      "sweltering"
  GROUP BY
      ROLLUP (1, 2, 4, 3)),
  sparkling as (
  SELECT
      CASE
  	WHEN grouping("vacuous"."all_sales_channel") = 1 THEN null
  	ELSE "vacuous"."all_sales_channel"
  	END as "channel",
      CASE
  	WHEN grouping("vacuous"."all_sales_item_brand_id") = 1 THEN null
  	ELSE "vacuous"."all_sales_item_brand_id"
  	END as "brand_id",
      CASE
  	WHEN grouping("vacuous"."all_sales_item_category_id") = 1 THEN null
  	ELSE "vacuous"."all_sales_item_category_id"
  	END as "category_id",
      CASE
  	WHEN grouping("vacuous"."all_sales_item_class_id") = 1 THEN null
  	ELSE "vacuous"."all_sales_item_class_id"
  	END as "class_id",
      count("vacuous"."all_sales_order_id") as "total_num_sales",
      sum("vacuous"."all_sales_quantity" * "vacuous"."all_sales_list_price") as "total_sales"
  FROM
      "vacuous"
  GROUP BY
      ROLLUP ("vacuous"."all_sales_channel", "vacuous"."all_sales_item_brand_id", "vacuous"."all_sales_item_class_id", "vacuous"."all_sales_item_category_id")),
  concerned as (
  SELECT
      avg("vacuous"."all_sales_quantity" * CASE WHEN year(cast("vacuous"."all_sales_date_date" as date)) BETWEEN 1999 AND 2001 THEN "vacuous"."all_sales_list_price" ELSE NULL END) as "overall_avg_sale"
  FROM
      "vacuous"),
  macho as (
  SELECT
      "vacuous"."all_sales_list_price" as "all_sales_list_price",
      "vacuous"."all_sales_quantity" as "all_sales_quantity",
      CASE
  	WHEN "late"."_virt_agg_grouping_5728540759741229" = 1 THEN null
  	ELSE coalesce("late"."all_sales_item_class_id","vacuous"."all_sales_item_class_id")
  	END as "class_id",
      CASE
  	WHEN "late"."_virt_agg_grouping_7230499189600468" = 1 THEN null
  	ELSE coalesce("late"."all_sales_item_brand_id","vacuous"."all_sales_item_brand_id")
  	END as "brand_id",
      CASE
  	WHEN "late"."_virt_agg_grouping_8745896301782811" = 1 THEN null
  	ELSE coalesce("late"."all_sales_item_category_id","vacuous"."all_sales_item_category_id")
  	END as "category_id",
      CASE
  	WHEN "late"."_virt_agg_grouping_9079213539192150" = 1 THEN null
  	ELSE coalesce("late"."all_sales_channel","vacuous"."all_sales_channel")
  	END as "channel"
  FROM
      "late"
      FULL JOIN "vacuous" on "late"."all_sales_channel" is not distinct from "vacuous"."all_sales_channel" AND "late"."all_sales_item_brand_id" is not distinct from "vacuous"."all_sales_item_brand_id" AND "late"."all_sales_item_category_id" is not distinct from "vacuous"."all_sales_item_category_id" AND "late"."all_sales_item_class_id" is not distinct from "vacuous"."all_sales_item_class_id"),
  scrawny as (
  SELECT
      "macho"."brand_id" as "brand_id",
      "macho"."category_id" as "category_id",
      "macho"."channel" as "channel",
      "macho"."class_id" as "class_id",
      sum("macho"."all_sales_quantity" * "macho"."all_sales_list_price") as "_virt_agg_sum_5574077946046721"
  FROM
      "macho"
  GROUP BY
      1,
      2,
      3,
      4),
  friendly as (
  SELECT
      "scrawny"."_virt_agg_sum_5574077946046721" as "_virt_agg_sum_5574077946046721",
      "sparkling"."total_sales" as "total_sales",
      coalesce("scrawny"."brand_id","sparkling"."brand_id") as "brand_id",
      coalesce("scrawny"."category_id","sparkling"."category_id") as "category_id",
      coalesce("scrawny"."channel","sparkling"."channel") as "channel",
      coalesce("scrawny"."class_id","sparkling"."class_id") as "class_id",
      coalesce("sparkling"."total_num_sales",0) as "total_num_sales"
  FROM
      "sparkling"
      FULL JOIN "scrawny" on "sparkling"."brand_id" is not distinct from "scrawny"."brand_id" AND "sparkling"."category_id" is not distinct from "scrawny"."category_id" AND "sparkling"."channel" is not distinct from "scrawny"."channel" AND "sparkling"."class_id" is not distinct from "scrawny"."class_id")
  SELECT
      "friendly"."channel" as "channel",
      "friendly"."brand_id" as "brand_id",
      "friendly"."class_id" as "class_id",
      "friendly"."category_id" as "category_id",
      "friendly"."total_sales" as "total_sales",
      coalesce("friendly"."total_num_sales",0) as "total_num_sales"
  FROM
      "friendly"
      INNER JOIN "concerned" on 1=1
  WHERE
      "friendly"."_virt_agg_sum_5574077946046721" > "concerned"."overall_avg_sale"

  ORDER BY
       'CATALOG'  asc nulls first,
      INVALID_REFERENCE_BUG asc nulls first,
      INVALID_REFERENCE_BUG asc nulls first,
      INVALID_REFERENCE_BUG asc nulls first
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy file read query23.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read query38.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: store_sales.customer_demographics.marital_status. Suggestions: ['store_sales.customer.demographics.marital_status', 'store_sales.return_customer.demographics.marital_status', 'store_sales.customer_demographic.marital_status', 'catalog_sales.ship_customer.demographics.marital_status', 'catalog_sales.bill_customer.demographics.marital_status', 'catalog_sales.bill_customer_demographic.marital_status']
  ```
- `trilogy run query64.preql`

  ```text
  Syntax error in query64.preql: Undefined concept: local.sale_year. Suggestions: ['sale_base.sale_year', 'base_agg.sale_base.sale_year', '_sale_base_sale_year']
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000

  GROUP BY
      1),
  abundant as (
  SELECT
      "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000

  GROUP BY
      1,
      coalesce("store_sales_date_date"."D_DATE_SK","store_sales_store_sales"."SS_SOLD_DATE_SK")),
  cooperative as (
  SELECT
      "cheerful"."store_sales_store_state" as "store_sales_store_state"
  FROM
      "cheerful"
  QUALIFY
      rank() over (order by INVALID_REFERENCE_BUG desc ) <= 5
  ),
  questionable as (
  SELECT
      "cooperative"."store_sales_store_state" as "top_states_store_sales_store_state"
  FROM
      "cooperative"),
  sweltering as (
  SELECT
      "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_store"."S_STATE" in (select questionable."top_states_store_sales_store_state" from questionable where questionable."top_states_store_sales_store_state" is not null)

  GROUP BY
      1,
      2,
      3,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_STORE_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER"),
  vacuous as (
  SELECT
      "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_store"."S_STATE" in (select questionable."top_states_store_sales_store_state" from questionable where questionable."top_states_store_sales_store_state" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER"),
  juicy as (
  SELECT
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "abundant"
      INNER JOIN "store" as "store_sales_store_store" on "abundant"."store_sales_store_id" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_store_store"."S_STATE" in (select questionable."top_states_store_sales_store_state" from questionable where questionable."top_states_store_sales_store_state" is not null)

  GROUP BY
      1,
      2),
  uneven as (
  SELECT
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "abundant"
      INNER JOIN "store" as "store_sales_store_store" on "abundant"."store_sales_store_id" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_store_store"."S_STATE" in (select questionable."top_states_store_sales_store_state" from questionable where questionable."top_states_store_sales_store_state" is not null)

  GROUP BY
      1,
      2,
      "abundant"."store_sales_store_id"),
  late as (
  SELECT
      "sweltering"."store_sales_store_county" as "store_sales_store_county",
      "sweltering"."store_sales_store_state" as "store_sales_store_state",
      grouping("sweltering"."store_sales_store_county") as "_virt_agg_grouping_8943320630755478",
      grouping("sweltering"."store_sales_store_state") as "_virt_agg_grouping_2294395731013452",
      sum("sweltering"."store_sales_net_profit") as "total_net_profit"
  FROM
      "sweltering"
  GROUP BY
      ROLLUP (2, 1)),
  concerned as (
  SELECT
      "vacuous"."store_sales_store_state" as "store_sales_store_state",
      sum("vacuous"."store_sales_net_profit") as "_virt_agg_sum_522117402712274"
  FROM
      "vacuous"
  GROUP BY
      1),
  yummy as (
  SELECT
      "uneven"."store_sales_store_county" as "store_sales_store_county",
      "uneven"."store_sales_store_state" as "store_sales_store_state",
      grouping("uneven"."store_sales_store_county") as "_virt_agg_grouping_4820390906191809",
      grouping("uneven"."store_sales_store_state") as "_virt_agg_grouping_7315987989213777"
  FROM
      "uneven"
  GROUP BY
      1,
      2),
  young as (
  SELECT
      "concerned"."_virt_agg_sum_522117402712274" as "_virt_agg_sum_522117402712274",
      "juicy"."store_sales_store_county" as "store_sales_store_county",
      "juicy"."store_sales_store_state" as "store_sales_store_state"
  FROM
      "juicy"
      LEFT OUTER JOIN "concerned" on "juicy"."store_sales_store_state" = "concerned"."store_sales_store_state"),
  sparkling as (
  SELECT
      coalesce("young"."store_sales_store_county","yummy"."store_sales_store_county") as "store_sales_store_county",
      coalesce("young"."store_sales_store_state","yummy"."store_sales_store_state") as "store_sales_store_state",
      rank() over (partition by "yummy"."_virt_agg_grouping_7315987989213777" + "yummy"."_virt_agg_grouping_4820390906191809" order by "young"."_virt_agg_sum_522117402712274" desc ) as "rnk"
  FROM
      "young"
      FULL JOIN "yummy" on "young"."store_sales_store_county" is not distinct from "yummy"."store_sales_store_county" AND "young"."store_sales_store_state" = "yummy"."store_sales_store_state")
  SELECT
      "late"."store_sales_store_state" as "store_sales_store_state",
      "late"."store_sales_store_county" as "store_sales_store_county",
      "late"."total_net_profit" as "total_net_profit",
      "late"."_virt_agg_grouping_2294395731013452" + "late"."_virt_agg_grouping_8943320630755478" as "hierarchy_level",
      "sparkling"."rnk" as "rnk"
  FROM
      "late"
      LEFT OUTER JOIN "sparkling" on "late"."store_sales_store_county" = "sparkling"."store_sales_store_county" AND "late"."store_sales_store_state" = "sparkling"."store_sales_store_state"
  ORDER BY
      "hierarchy_level" desc,
      "late"."store_sales_store_state" asc,
      "sparkling"."rnk" asc
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000

  GROUP BY
      1),
  abundant as (
  SELECT
      "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
      "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id"
  FROM
      "store_sales" as "store_sales_store_sales"
  GROUP BY
      1,
      2),
  cooperative as (
  SELECT
      "cheerful"."store_sales_store_state" as "store_sales_store_state"
  FROM
      "cheerful"
  QUALIFY
      rank() over (order by INVALID_REFERENCE_BUG desc ) <= 5
  ),
  questionable as (
  SELECT
      "cooperative"."store_sales_store_state" as "top_states_store_sales_store_state"
  FROM
      "cooperative"),
  sparkling as (
  SELECT
      "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_store"."S_STATE" in (select questionable."top_states_store_sales_store_state" from questionable where questionable."top_states_store_sales_store_state" is not null)

  GROUP BY
      1,
      2,
      3,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER"),
  juicy as (
  SELECT
      "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_store"."S_STATE" in (select questionable."top_states_store_sales_store_state" from questionable where questionable."top_states_store_sales_store_state" is not null)

  GROUP BY
      1,
      2,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER"),
  uneven as (
  SELECT
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "abundant"
      INNER JOIN "date_dim" as "store_sales_date_date" on "abundant"."store_sales_date_id" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "abundant"."store_sales_store_id" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_store"."S_STATE" in (select questionable."top_states_store_sales_store_state" from questionable where questionable."top_states_store_sales_store_state" is not null)

  GROUP BY
      1,
      2,
      "abundant"."store_sales_store_id"),
  abhorrent as (
  SELECT
      "sparkling"."store_sales_store_county" as "store_sales_store_county",
      "sparkling"."store_sales_store_state" as "store_sales_store_state",
      sum("sparkling"."store_sales_net_profit") as "total_net_profit"
  FROM
      "sparkling"
  GROUP BY
      ROLLUP (2, 1)),
  vacuous as (
  SELECT
      "juicy"."store_sales_store_state" as "store_sales_store_state",
      sum("juicy"."store_sales_net_profit") as "_virt_agg_sum_522117402712274"
  FROM
      "juicy"
  GROUP BY
      1),
  yummy as (
  SELECT
      "uneven"."store_sales_store_county" as "store_sales_store_county",
      "uneven"."store_sales_store_state" as "store_sales_store_state",
      grouping("uneven"."store_sales_store_county") as "g_county",
      grouping("uneven"."store_sales_store_state") as "g_state"
  FROM
      "uneven"
  GROUP BY
      ROLLUP (2, 1)),
  concerned as (
  SELECT
      "yummy"."g_state" + "yummy"."g_county" as "level",
      "yummy"."store_sales_store_county" as "store_sales_store_county",
      "yummy"."store_sales_store_state" as "store_sales_store_state",
      rank() over (partition by "yummy"."g_state" + "yummy"."g_county",CASE
  	WHEN "yummy"."g_county" = 0 THEN "yummy"."store_sales_store_state"
  	ELSE null
  	END order by "vacuous"."_virt_agg_sum_522117402712274" desc ) as "rnk"
  FROM
      "yummy"
      LEFT OUTER JOIN "vacuous" on "yummy"."store_sales_store_state" = "vacuous"."store_sales_store_state")
  SELECT
      "abhorrent"."store_sales_store_state" as "store_sales_store_state",
      "abhorrent"."store_sales_store_county" as "store_sales_store_county",
      "abhorrent"."total_net_profit" as "total_net_profit",
      "concerned"."level" as "level",
      "concerned"."rnk" as "rnk"
  FROM
      "abhorrent"
      LEFT OUTER JOIN "concerned" on "abhorrent"."store_sales_store_county" = "concerned"."store_sales_store_county" AND "abhorrent"."store_sales_store_state" = "concerned"."store_sales_store_state"
  ORDER BY
      "concerned"."level" desc,
      "abhorrent"."store_sales_store_state" asc,
      "concerned"."rnk" asc
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy run query70.preql`

  ```text
  Syntax error in query70.preql: Output column 'rnk' renames 'local.rnk' back to the name of an existing concept 'rnk' (defined at line 22) that 'local.rnk' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'rnk_out').
  ```
- `trilogy run query70.preql`

  ```text
  Unexpected error in query70.preql: Invalid reference string found in query:
  WITH
  cheerful as (
  SELECT
      "store_sales_store_store"."S_STATE" as "_top_states_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      LEFT OUTER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000

  GROUP BY
      1),
  cooperative as (
  SELECT
      "cheerful"."_top_states_state" as "_top_states_state"
  FROM
      "cheerful"
  QUALIFY
      rank() over (order by INVALID_REFERENCE_BUG desc ) <= 5
  ),
  questionable as (
  SELECT
      "cooperative"."_top_states_state" as "top_states_state"
  FROM
      "cooperative"),
  abundant as (
  SELECT
      "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
      "store_sales_store_store"."S_COUNTY" as "store_sales_store_county",
      "store_sales_store_store"."S_STATE" as "store_sales_store_state"
  FROM
      "store_sales" as "store_sales_store_sales"
      INNER JOIN "date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
      INNER JOIN "store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
  WHERE
      "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_store_store"."S_STATE" in (select questionable."top_states_state" from questionable where questionable."top_states_state" is not null)

  GROUP BY
      1,
      2,
      3,
      "store_sales_store_sales"."SS_ITEM_SK",
      "store_sales_store_sales"."SS_STORE_SK",
      "store_sales_store_sales"."SS_TICKET_NUMBER"),
  uneven as (
  SELECT
      "abundant"."store_sales_store_county" as "store_sales_store_county",
      "abundant"."store_sales_store_state" as "store_sales_store_state",
      CASE
  	WHEN grouping("abundant"."store_sales_store_county") = 0 THEN "abundant"."store_sales_store_state"
  	ELSE null
  	END as "parent",
      grouping("abundant"."store_sales_store_state") + grouping("abundant"."store_sales_store_county") as "level",
      sum("abundant"."store_sales_net_profit") as "total_net_profit"
  FROM
      "abundant"
  GROUP BY
      ROLLUP (2, 1)),
  yummy as (
  SELECT
      "uneven"."level" as "level",
      "uneven"."store_sales_store_county" as "store_sales_store_county",
      "uneven"."store_sales_store_state" as "store_sales_store_state",
      "uneven"."total_net_profit" as "total_net_profit",
      rank() over (partition by "uneven"."level","uneven"."parent" order by "uneven"."total_net_profit" desc ) as "rnk"
  FROM
      "uneven")
  SELECT
      "yummy"."store_sales_store_state" as "store_sales_store_state",
      "yummy"."store_sales_store_county" as "store_sales_store_county",
      "yummy"."total_net_profit" as "total_net_profit",
      "yummy"."level" as "hierarchy_level",
      "yummy"."rnk" as "rnk"
  FROM
      "yummy"
  ORDER BY
      "hierarchy_level" desc,
      "yummy"."store_sales_store_state" asc,
      "yummy"."rnk" asc
  LIMIT (100), this should never occur. Please create an issue to report this.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Ambiguous reference 'other_nr.the_year': matches ['other_nr.catalog_nr.the_year', 'other_nr.web_nr.the_year']. Qualify the full path to disambiguate.
  ```
- `trilogy run query78.preql`

  ```text
  Syntax error in query78.preql: Undefined concept: store_nr.web_nr.the_year. Suggestions: ['store_nr.the_year', 'web_nr.the_year', 'catalog_nr.the_year', 'store_nr.catalog_nr.the_year', '_store_nr_the_year', 'store_nr.web_nr.web_qty']
  ```

### `syntax-parse`

- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.item.brand_id, sum(all_sales.quantity * all_sales.list_price) as total_sales…ng sum(all_sales.quantity * all_sales.list_price) > 4398 order by all_sales.channel asc nulls first, all_sales.item.brand_id asc nulls first limit 5;`

  ```text
  Syntax error in stdin:  --> 2:485
    |
  2 | select all_sales.channel, all_sales.item.brand_id, sum(all_sales.quantity * all_sales.list_price) as total_sales where year(all_sales.date.date::date) = 2001 and all_sales.date.month_of_year = 11 and count_distinct(all_sales.channel ? year(all_sales.date.date::date) between 1999 and 2001) by (all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id) = 3 by rollup (all_sales.channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id) having sum(all_sales.quantity * all_sales.list_price) > 4398 order by all_sales.channel asc nulls first, all_sales.item.brand_id asc nulls first limit 5;
    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     ^---
    |
    = expected limit or order_by
  Location:
  ..., all_sales.item.category_id) ??? having sum(all_sales.quantity
  ```
- `trilogy run --import raw.all_sales:all_sales select all_sales.channel, all_sales.item.brand_id, sum(all_sales.quantity * all_sales.list_price) as total_sales…ng sum(all_sales.quantity * all_sales.list_price) > 4398 order by all_sales.channel asc nulls first, all_sales.item.brand_id asc nulls first limit 5;`

  ```text
  Syntax error in stdin:  --> 2:485
    |
  2 | select all_sales.channel, all_sales.item.brand_id, sum(all_sales.quantity * all_sales.list_price) as total_sales where year(all_sales.date.date::date) = 2001 and all_sales.date.month_of_year = 11 and count_distinct(all_sales.channel ? year(all_sales.date.date::date) between 1999 and 2001) by (all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id) = 3 by rollup (all_sales.channel, all_sales.item.brand_id, all_sales.item.class_id, all_sales.item.category_id) having sum(all_sales.quantity * all_sales.list_price) > 4398 order by all_sales.channel asc nulls first, all_sales.item.brand_id asc nulls first limit 5;
    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     ^---
    |
    = expected limit or order_by
  Location:
  ..., all_sales.item.category_id) ??? having sum(all_sales.quantity
  ```
- `trilogy file write query38.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Get distinct (last_na…rst_name = cs_combos.first_name
    = ws_combos.first_name
inner join ss_combos.sale_date = cs_combos.sale_date
    = ws_combos.sale_date
limit 100;
`

  ```text
  refused to write 'query38.preql': not syntactically valid Trilogy.

  Parse error:
    --> 33:28
     |
  33 |         ss_combos.last_name,
     |                            ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...t(         ss_combos.last_name ??? ,         ss_combos.first_name...

  Write stats: received 1073 chars / 1073 bytes; tail: …' = cs_combos.sale_date\\n    = ws_combos.sale_date\\nlimit 100;\\n'.
  ```
- `trilogy file write query64.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.catalog_returns as ca…1999.line_count_99
order by y1999.prod_name_99 asc, y1999.store_name_99 asc, y2000.line_count_00 asc, y1999.wholesale_99 asc, y2000.wholesale_00 asc;`

  ```text
  refused to write 'query64.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument — Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a key field: `count(<key>)` (counts are already distinct) — e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
   sale_base.first_ship_yr,     ??? count(*) as sale_line_count,

  Write stats: received 8612 chars / 8612 bytes; tail: …'ount_00 asc, y1999.wholesale_99 asc, y2000.wholesale_00 asc;'.
  ```
- `trilogy run - --import raw.store_sales:store_sales`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY — remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
   store_sales.date.year = 2000 ??? group by store_sales.store.sta...
  ```

### `undefined-concept`

- `trilogy explore query14.preql`

  ```text
  Failed to parse query14.preql: (UndefinedConceptException(...), "Ambiguous reference 'nov2001_detail.brand_id': matches ['nov2001_detail.all_sales.item.brand_id', 'nov2001_detail.qualifying_combos.brand_id']. Qualify the full path to disambiguate.")
  ```

### `cli-misuse`

- `trilogy explore raw/date_dim.preql`

  ```text
  Invalid value for 'PATH': File 'raw/date_dim.preql' does not exist.
  ```
