# Trilogy failure analysis — 20260820-031801

- Run `20260820-031800_ingest_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1394 | failed: 131 (9%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 60 | 46% |
| `disabled-tool` | 35 | 27% |
| `syntax-parse` | 20 | 15% |
| `undefined-concept` | 6 | 5% |
| `join-resolution` | 5 | 4% |
| `type-error` | 2 | 2% |
| `cli-misuse` | 2 | 2% |
| `syntax-missing-alias` | 1 | 1% |

## Detail

### `other`

- `trilogy file write probe14.preql --run-and-delete`

  ```text
  Resolution error in probe14.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ws.web_site.web_site_at_ws_web_site_site_sk_grouped_by_ws.web_site.site_id_at_ws_web_site_site_id_at_ws_web_site_site_id onto ws.item.item_at_ws_item_item_sk_at_ws_item_item_sk, ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.order_number_at_ws_order_number_at_ws_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe23.preql --run-and-delete`

  ```text
  Resolution error in probe23.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ws.web_site.web_site_at_ws_web_site_site_sk_grouped_by_ws.web_site.site_id_at_ws_web_site_site_id_at_ws_web_site_site_id onto ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.order_number_at_ws_order_number_at_ws_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe23.preql --run-and-delete`

  ```text
  Resolution error in probe23.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ws.web_site.web_site_at_ws_web_site_site_sk_grouped_by_ws.web_site.site_id_at_ws_web_site_site_id_at_ws_web_site_site_id onto ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.order_number_at_ws_order_number_at_ws_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe24.preql --run-and-delete`

  ```text
  Syntax error in probe24.preql: union(...) requires at least two relational arms.
  ```
- `trilogy file write probe28.preql --run-and-delete`

  ```text
  Resolution error in probe28.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: wr.date_dim.date_dim_at_wr_date_dim_date_sk_join_wr.item.item_at_wr_item_item_sk_join_wr.web_returns_at_wr_item_item_sk_wr_order_number_at_wr_item_item_sk_wr_order_number_filtered_by_5271196116334975_at_wr_item_item_sk_wr_order_number_at_ret_i_sk_ret_ord onto ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.item.item_sk_ws.web_site.site_sk_at_ws_item_item_sk_ws_web_site_site_sk_join_ws.web_site.web_site_at_ws_web_site_site_sk_at_ws_item_item_sk_ws_web_site_site_sk_filtered_by_6451768858066118, ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.item.item_sk_ws.web_site.site_sk_at_ws_item_item_sk_ws_web_site_site_sk_join_ws.web_site.web_site_at_ws_web_site_site_sk_at_ws_item_item_sk_ws_web_site_site_sk_filtered_by_6451768858066118_at_ws_item_item_sk_ws_web_site_site_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe28.preql --run-and-delete`

  ```text
  Resolution error in probe28.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ws.web_site.web_site_at_ws_web_site_site_sk_grouped_by_ws.web_site.site_id_at_ws_web_site_site_id_at_ws_web_site_site_id onto ws.item.item_at_ws_item_item_sk_at_ws_item_item_sk, ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.order_number_at_ws_order_number_at_ws_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Unexpected error in probe2.preql: Could not render the query: Missing source reference to ws.sold_date.year; Missing source reference to ws.sold_date.moy; Missing source reference to ws.bill_customer.customer_sk; Missing source reference to c.customer_sk; Missing source reference to cs.sold_date.year; Missing source reference to cs.sold_date.moy; Missing source reference to cs.ship_customer.customer_sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  wakeful as (
  SELECT
      CASE WHEN ( INVALID_REFERENCE_BUG<Missing source reference to ws.sold_date.year> = 2002 and INVALID_REFERENCE_BUG<Missing source reference to ws.sold_date.moy> >= 1 and INVALID_REFERENCE_BUG<Missing source reference to ws.sold_date.moy> <= 4 ) THEN INVALID_REFERENCE_BUG<Missing source reference to ws.bill_customer.customer_sk> ELSE NULL END as "web_buyers"
  ),
  thoughtful as (
  SELECT
      CASE WHEN exists (select 1 from wakeful where wakeful."web_buyers" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk>) THEN INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk> ELSE NULL END as "_virt_filter_customer_sk_4280145623331090",
      INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk> as "c_customer_sk"
  FROM
      "wakeful"),
  quizzical as (
  SELECT
      CASE WHEN ( INVALID_REFERENCE_BUG<Missing source reference to cs.sold_date.year> = 2002 and INVALID_REFERENCE_BUG<Missing source reference to cs.sold_date.moy> >= 1 and INVALID_REFERENCE_BUG<Missing source reference to cs.sold_date.moy> <= 4 ) THEN INVALID_REFERENCE_BUG<Missing source reference to cs.ship_customer.customer_sk> ELSE NULL END as "catalog_buyers"
  ),
  cheerful as (
  SELECT
      CASE WHEN ( exists (select 1 from wakeful where wakeful."web_buyers" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk>) or exists (select 1 from quizzical where quizzical."catalog_buyers" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk>) ) THEN INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk> ELSE NULL END as "_virt_filter_customer_sk_6681681566330654",
      INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk> as "c_customer_sk"
  FROM
      "wakeful"),
  highfalutin as (
  SELECT
      CASE WHEN exists (select 1 from quizzical where quizzical."catalog_buyers" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk>) THEN INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk> ELSE NULL END as "_virt_filter_customer_sk_6685857326031211",
      INVALID_REFERENCE_BUG<Missing source reference to c.customer_sk> as "c_customer_sk"
  FROM
      "quizzical"),
  cooperative as (
  SELECT
      "cheerful"."_virt_filter_customer_sk_6681681566330654" as "_virt_filter_customer_sk_6681681566330654",
      "highfalutin"."_virt_filter_customer_sk_6685857326031211" as "_virt_filter_customer_sk_6685857326031211",
      "highfalutin"."c_customer_sk" as "c_customer_sk",
      "thoughtful"."_virt_filter_customer_sk_4280145623331090" as "_virt_filter_customer_sk_4280145623331090"
  FROM
      "thoughtful"
      INNER JOIN "cheerful" on "thoughtful"."c_customer_sk" = "cheerful"."c_customer_sk"
      INNER JOIN "highfalutin" on "thoughtful"."c_customer_sk" = "highfalutin"."c_customer_sk"
  GROUP BY
      1,
      2,
      3,
      4)
  SELECT
      count("cooperative"."c_customer_sk") as "with_store",
      count("cooperative"."_virt_filter_customer_sk_4280145623331090") as "web",
      count("cooperative"."_virt_filter_customer_sk_6685857326031211") as "cat",
      count("cooperative"."_virt_filter_customer_sk_6681681566330654") as "web_or_cat"
  FROM
      "cooperative"
  ```
- `trilogy file write probe11.preql --run-and-delete`

  ```text
  Resolution error in probe11.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk onto ss.customer.customer_at_ss_customer_customer_sk_at_ss_customer_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe12.preql --run-and-delete`

  ```text
  {
    "event": "write",
    "path": "probe12.preql",
    "bytes": 1335
  }
  {
    "event": "output_truncated",
    "dropped_events": 0,
    "note": "Output exceeded the tool cap; intermediate events dropped (any error/summary events are preserved below). Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  {
    "event": "error",
    "message": "Syntax error in probe12.preql: Missing source map entry for ss.customer.customer_sk with pseudonyms set(), have map: {'cs.bill_customer.customer_sk': {BuildDatasource(name='customer', columns=[BuildColumnAssignment(alias='c_customer_sk', concept=cs.bill_cus
  …
  ents={'cs.bill_customer.customer_sk'}, where_clause=None, _str='Grain<cs.bill_customer.customer_sk>', _str_no_condition='Grain<cs.bill_customer.customer_sk>', abstract=False), namespace='cs.bill_customer', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'cs.bill_customer.last_review_date.date_sk', 'cs.bill_customer.first_shipto_date.date_sk', 'cs.bill_customer.customer_demographics.demo_sk', 'cs.bill_customer.first_sales_date.date_sk', 'cs.bill_customer.customer_address.address_sk'})}}"
  }
  ```
- `trilogy file write probe17.preql --run-and-delete`

  ```text
  Resolution error in probe17.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.ticket_number_at_sr_ticket_number_at_sr_ticket_number onto sr.customer.customer_at_sr_customer_customer_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.customer.customer_sk_sr.item.item_sk_at_sr_customer_customer_sk_sr_item_item_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.customer.customer_sk_ss.date_dim.date_sk_ss.item.item_sk_at_ss_customer_customer_sk_ss_date_dim_date_sk_ss_item_item_sk_at_ss_customer_customer_sk_ss_date_dim_date_sk_ss_item_item_sk_filtered_by_3104508520415000_at_ss_customer_customer_sk_ss_date_dim_date_sk_ss_item_item_sk_grouped_by_local.___tvf_arm_0_customer_sk_local.___tvf_arm_0_item_sk_at_local____tvf_arm_0_customer_sk_local____tvf_arm_0_item_sk_at_local____tvf_arm_0_customer_sk_local____tvf_arm_0_item_sk_union_sr.customer.customer_at_sr_customer_customer_sk_join_sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.customer.customer_sk_sr.date_dim.date_sk_sr.item.item_sk_at_sr_customer_customer_sk_sr_date_dim_date_sk_sr_item_item_sk_at_sr_customer_customer_sk_sr_date_dim_date_sk_sr_item_item_sk_filtered_by_5504355720451390_at_sr_customer_customer_sk_sr_date_dim_date_sk_sr_item_item_sk_grouped_by_local.___tvf_arm_1_customer_sk_local.___tvf_arm_1_item_sk_at_local____tvf_arm_1_customer_sk_local____tvf_arm_1_item_sk_at_local____tvf_arm_1_customer_sk_local____tvf_arm_1_item_sk_union_cs.bill_customer.customer_at_cs_bill_customer_customer_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.bill_customer.customer_sk_cs.item.item_sk_cs.sold_date.date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_join_cs.sold_date.date_dim_at_cs_sold_date_date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_filtered_by_3939203757034175_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_grouped_by_local.___tvf_arm_2_customer_sk_local.___tvf_arm_2_item_sk_at_local____tvf_arm_2_customer_sk_local____tvf_arm_2_item_sk_at_local____tvf_arm_2_customer_sk_local____tvf_arm_2_item_sk_intersected_at_matched_customer_sk_matched_item_sk_grouped_by_sr.customer.customer_sk_sr.item.item_sk_at_sr_customer_customer_sk_sr_item_item_sk_filtered_by_1086235146724492_at_sr_customer_customer_sk_sr_item_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe19.preql --run-and-delete`

  ```text
  Resolution error in probe19.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.date_dim.date_sk_ss.ticket_number_at_ss_date_dim_date_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_ticket_number_filtered_by_6143679453612325 onto sr.customer.customer_at_sr_customer_customer_sk_join_sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.customer.customer_sk_ss.date_dim.date_sk_ss.item.item_sk_at_ss_customer_customer_sk_ss_date_dim_date_sk_ss_item_item_sk_at_ss_customer_customer_sk_ss_date_dim_date_sk_ss_item_item_sk_filtered_by_3104508520415000_at_ss_customer_customer_sk_ss_date_dim_date_sk_ss_item_item_sk_grouped_by_local.___tvf_arm_0_customer_sk_local.___tvf_arm_0_item_sk_at_local____tvf_arm_0_customer_sk_local____tvf_arm_0_item_sk_at_local____tvf_arm_0_customer_sk_local____tvf_arm_0_item_sk_union_sr.customer.customer_at_sr_customer_customer_sk_join_sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.customer.customer_sk_sr.date_dim.date_sk_sr.item.item_sk_at_sr_customer_customer_sk_sr_date_dim_date_sk_sr_item_item_sk_at_sr_customer_customer_sk_sr_date_dim_date_sk_sr_item_item_sk_filtered_by_5504355720451390_at_sr_customer_customer_sk_sr_date_dim_date_sk_sr_item_item_sk_grouped_by_local.___tvf_arm_1_customer_sk_local.___tvf_arm_1_item_sk_at_local____tvf_arm_1_customer_sk_local____tvf_arm_1_item_sk_at_local____tvf_arm_1_customer_sk_local____tvf_arm_1_item_sk_union_cs.bill_customer.customer_at_cs_bill_customer_customer_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.bill_customer.customer_sk_cs.item.item_sk_cs.sold_date.date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_join_cs.sold_date.date_dim_at_cs_sold_date_date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_filtered_by_3939203757034175_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_grouped_by_local.___tvf_arm_2_customer_sk_local.___tvf_arm_2_item_sk_at_local____tvf_arm_2_customer_sk_local____tvf_arm_2_item_sk_at_local____tvf_arm_2_customer_sk_local____tvf_arm_2_item_sk_intersected_at_matched_customer_sk_matched_item_sk_grouped_by_sr.customer.customer_sk_sr.date_dim.date_sk_sr.date_dim.year_sr.item.item_sk_sr.ticket_number_at_sr_item_item_sk_sr_ticket_number_filtered_by_4222472790372969_at_sr_item_item_sk_sr_ticket_number_at_matched_r_ck_matched_r_ik_matched_r_rtk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_ss.preql --run-and-delete`

  ```text
  Resolution error in probe_ss.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_grouped_by_ss.date_dim.date_ss.date_dim.moy_ss.date_dim.year_at_ss_date_dim_date_ss_date_dim_moy_ss_date_dim_year_at_ss_date_dim_date_ss_date_dim_moy_ss_date_dim_year onto ss.customer.customer_at_ss_customer_customer_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_sr.preql --run-and-delete`

  ```text
  Resolution error in probe_sr.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.date_dim.date_dim_at_sr_date_dim_date_sk_grouped_by_sr.date_dim.date_sr.date_dim.moy_sr.date_dim.year_at_sr_date_dim_date_sr_date_dim_moy_sr_date_dim_year_at_sr_date_dim_date_sr_date_dim_moy_sr_date_dim_year onto sr.customer.customer_at_sr_customer_customer_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_cs.preql --run-and-delete`

  ```text
  Resolution error in probe_cs.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.sold_date.date_dim_at_cs_sold_date_date_sk_grouped_by_cs.sold_date.date_cs.sold_date.moy_cs.sold_date.year_at_cs_sold_date_date_cs_sold_date_moy_cs_sold_date_year_at_cs_sold_date_date_cs_sold_date_moy_cs_sold_date_year onto cs.bill_customer.customer_at_cs_bill_customer_customer_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_at_cs_item_item_sk_cs_order_number_at_cs_item_item_sk_cs_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_ss2.preql --run-and-delete`

  ```text
  Resolution error in probe_ss2.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_grouped_by_ss.date_dim.date_at_ss_date_dim_date_at_ss_date_dim_date onto ss.customer.customer_at_ss_customer_customer_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number, ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_ss3.preql --run-and-delete`

  ```text
  Resolution error in probe_ss3.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk onto ss.date_dim.date_dim_at_ss_date_dim_date_sk_grouped_by_ss.date_dim.date_at_ss_date_dim_date_at_ss_date_dim_date. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_ss4.preql --run-and-delete`

  ```text
  Resolution error in probe_ss4.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.store.store_at_ss_store_store_sk_grouped_by_ss.store.store_id_ss.store.store_name_at_ss_store_store_id_ss_store_store_name_at_ss_store_store_id_ss_store_store_name onto ss.customer.customer_at_ss_customer_customer_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number, ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_G3.preql --run-and-delete`

  ```text
  Resolution error in probe_G3.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.bill_customer.customer_at_cs_bill_customer_customer_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_join_cs.sold_date.date_dim_at_cs_sold_date_date_sk_at_cs_item_item_sk_cs_order_number_filtered_by_3492176634144671_at_cs_item_item_sk_cs_order_number_at_cs_rows_cust_sk_cs_rows_item_sk_cs_rows_order_number_join_sr.customer.customer_at_sr_customer_customer_sk_join_sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_filtered_by_4584242755083314_at_sr_item_item_sk_sr_ticket_number_at_sr_rows_cust_sk_sr_rows_item_sk_sr_rows_ticket_at_cs_rows_cust_sk_cs_rows_item_sk_cs_rows_order_number_sr_rows_ticket_at_cs_rows_cust_sk_cs_rows_item_sk_cs_rows_order_number_sr_rows_ticket_join_sr.customer.customer_at_sr_customer_customer_sk_join_sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_filtered_by_4584242755083314_at_sr_item_item_sk_sr_ticket_number_at_sr_rows_cust_sk_sr_rows_item_sk_sr_rows_ticket_at_sr_rows_cust_sk_sr_rows_item_sk_sr_rows_ticket_join_sr.customer.customer_at_sr_customer_customer_sk_join_sr.date_dim.date_dim_at_sr_date_dim_date_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_filtered_by_4584242755083314_at_sr_item_item_sk_sr_ticket_number_at_sr_rows_cust_sk_sr_rows_item_sk_sr_rows_ticket_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_1936709882920341_at_ss_item_item_sk_ss_ticket_number_at_ss_rows_cust_sk_ss_rows_item_sk_ss_rows_store_sk_ss_rows_ticket_at_sr_rows_cust_sk_sr_rows_item_sk_sr_rows_ticket_ss_rows_store_sk_at_sr_rows_cust_sk_sr_rows_item_sk_sr_rows_ticket_ss_rows_store_sk_grouped_by_local._matched_cs_net_profit_local._matched_item_sk_local._matched_sr_net_loss_local._matched_ss_net_profit_local._matched_store_sk_local._matched_ticket_at_local__matched_cs_net_profit_local__matched_item_sk_local__matched_sr_net_loss_local__matched_store_sk_local__matched_ticket_filtered_by_655943576475985_at_matched_cs_net_profit_matched_item_sk_matched_sr_net_loss_matched_store_sk_matched_ticket_at_matched_cs_net_profit_matched_item_sk_matched_sr_net_loss_matched_store_sk_matched_ticket onto item.item_at_item_item_sk_grouped_by_item.item_desc_item.item_id_at_item_item_desc_item_item_id_at_item_item_desc_item_item_id. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_R.preql --run-and-delete`

  ```text
  Syntax error in probe_R.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 5, column 1)
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  {
    "event": "write",
    "path": "probe_check.preql",
    "bytes": 1660
  }
  {
    "event": "output_truncated",
    "dropped_events": 0,
    "note": "Output exceeded the tool cap; intermediate events dropped (any error/summary events are preserved below). Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  {
    "event": "error",
    "message": "Syntax error in probe_check.preql: Missing source map entry for cs.bill_customer.first_name with pseudonyms set(), have map: {'ws.bill_customer.customer_sk': {BuildDatasource(name='customer', columns=[BuildColumnAssignment(alias='c_customer_sk', concept
  …
  ents={'ws.bill_customer.customer_sk'}, where_clause=None, _str='Grain<ws.bill_customer.customer_sk>', _str_no_condition='Grain<ws.bill_customer.customer_sk>', abstract=False), namespace='ws.bill_customer', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'ws.bill_customer.customer_demographics.demo_sk', 'ws.bill_customer.first_shipto_date.date_sk', 'ws.bill_customer.first_sales_date.date_sk', 'ws.bill_customer.last_review_date.date_sk', 'ws.bill_customer.customer_address.address_sk'})}}"
  }
  ```
- `trilogy file write probe_374591292.preql --run-and-delete`

  ```text
  Syntax error in probe_374591292.preql: Output column 'bench_profit' renames 'local.bench_profit' back to the name of an existing concept 'bench_profit' (defined at line 4) that 'local.bench_profit' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'bench_profit_out').
  ```
- `trilogy file write probe_374591292.preql --run-and-delete`

  ```text
  Unexpected error in probe_374591292.preql:
  ```
- `trilogy file write probe_374591292.preql --run-and-delete`

  ```text
  Unexpected error in probe_374591292.preql:
  ```
- `trilogy file write probe_374591292.preql --run-and-delete`

  ```text
  Unexpected error in probe_374591292.preql:
  ```
- `trilogy file write probe_374591292.preql --run-and-delete`

  ```text
  Unexpected error in probe_374591292.preql:
  ```
- `trilogy file write probe_374591292.preql --run-and-delete`

  ```text
  Unexpected error in probe_374591292.preql:
  ```
- `trilogy file write probe_374591292.preql --run-and-delete`

  ```text
  Unexpected error in probe_374591292.preql:
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_grouped_by_ss.customer.customer_address.county_ss.customer.customer_address.state_at_ss_customer_customer_address_county_ss_customer_customer_address_state onto cs.bill_customer.customer_at_cs_bill_customer_customer_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.bill_customer.customer_sk_cs.item.item_sk_cs.sold_date.date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_join_cs.item.item_at_cs_item_item_sk_join_cs.sold_date.date_dim_at_cs_sold_date_date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_filtered_by_5982767587549443_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_grouped_by_local.___tvf_arm_0_customer_sk_at_local____tvf_arm_0_customer_sk_at_local____tvf_arm_0_customer_sk_union_ws.bill_customer.customer_at_ws_bill_customer_customer_sk_join_ws.item.item_at_ws_item_item_sk_join_ws.sold_date.date_dim_at_ws_sold_date_date_sk_join_ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.bill_customer.customer_sk_ws.item.item_sk_ws.sold_date.date_sk_at_ws_bill_customer_customer_sk_ws_item_item_sk_ws_sold_date_date_sk_at_ws_bill_customer_customer_sk_ws_item_item_sk_ws_sold_date_date_sk_filtered_by_4828137868064177_at_ws_bill_customer_customer_sk_ws_item_item_sk_ws_sold_date_date_sk_grouped_by_local.___tvf_arm_1_customer_sk_at_local____tvf_arm_1_customer_sk_at_local____tvf_arm_1_customer_sk_unioned_at_qual_customers_customer_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.customer.customer_sk_ss.date_dim.date_sk_ss.date_dim.month_seq_ss.ext_sales_price_ss.item.item_sk_ss.store.county_ss.store.state_ss.store.store_sk_ss.ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_5295738423486433, cs.bill_customer.customer_at_cs_bill_customer_customer_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.bill_customer.customer_sk_cs.item.item_sk_cs.sold_date.date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_join_cs.item.item_at_cs_item_item_sk_join_cs.sold_date.date_dim_at_cs_sold_date_date_sk_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_filtered_by_5982767587549443_at_cs_bill_customer_customer_sk_cs_item_item_sk_cs_sold_date_date_sk_grouped_by_local.___tvf_arm_0_customer_sk_at_local____tvf_arm_0_customer_sk_at_local____tvf_arm_0_customer_sk_union_ws.bill_customer.customer_at_ws_bill_customer_customer_sk_join_ws.item.item_at_ws_item_item_sk_join_ws.sold_date.date_dim_at_ws_sold_date_date_sk_join_ws.web_sales_at_ws_item_item_sk_ws_order_number_grouped_by_ws.bill_customer.customer_sk_ws.item.item_sk_ws.sold_date.date_sk_at_ws_bill_customer_customer_sk_ws_item_item_sk_ws_sold_date_date_sk_at_ws_bill_customer_customer_sk_ws_item_item_sk_ws_sold_date_date_sk_filtered_by_4828137868064177_at_ws_bill_customer_customer_sk_ws_item_item_sk_ws_sold_date_date_sk_grouped_by_local.___tvf_arm_1_customer_sk_at_local____tvf_arm_1_customer_sk_at_local____tvf_arm_1_customer_sk_unioned_at_qual_customers_customer_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by___preql_internal.all_rows_ss.customer.customer_sk_ss.date_dim.date_sk_ss.date_dim.month_seq_ss.ext_sales_price_ss.item.item_sk_ss.ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_5295738423486433_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_grouped_by__at_abstract_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_7733512100258821_grouped_by_ss.customer.customer_sk_at_ss_customer_customer_sk_at_ss_customer_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_grouped_by_ss.customer.customer_address.county_ss.customer.customer_address.state_at_ss_customer_customer_address_county_ss_customer_customer_address_state onto ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_9331849815651580, ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_9331849815651580_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_grouped_by__at_abstract_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_7733512100258821_grouped_by_ss.customer.customer_sk_at_ss_customer_customer_sk_at_ss_customer_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Resolution error in probe6.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_grouped_by_ss.customer.customer_address.county_ss.customer.customer_address.state_at_ss_customer_customer_address_county_ss_customer_customer_address_state onto ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_9331849815651580, ss.customer.customer_at_ss_customer_customer_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_9331849815651580_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_grouped_by__at_abstract_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_7733512100258821_grouped_by_ss.customer.customer_sk_at_ss_customer_customer_sk_at_ss_customer_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Cannot compare DATE (ref:d.date) and STRING (1999-12-25) of different types with operator >= in ref:d.date >= 1999-12-25
  ```
- `trilogy file write answer_1484301313.preql --run`

  ```text
  Syntax error in answer_1484301313.preql: Impossible comparison in ref:ss.promotion.channel_email = Y: 'Y' can never match a declared value of enum<'N'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_1484301313.preql --run`

  ```text
  Syntax error in answer_1484301313.preql: Impossible comparison in ref:ss.promotion.channel_email = Y: 'Y' can never match a declared value of enum<'N'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_4037365554.preql --run`

  ```text
  Syntax error in answer_4037365554.preql: Output column 'monthly_total' renames 'local.monthly_total' back to the name of an existing concept 'monthly_total' (defined at line 3) that 'local.monthly_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'monthly_total_out').
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: WHERE input(s) ['ss.item.current_price'] cannot be related to the query outputs ['local.sale_cnt', 'ss.date_dim.year', 'ss.item.item_sk']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: WHERE input(s) ['ss.item.current_price'] cannot be related to the query outputs ['local.sale_cnt', 'ss.date_dim.year', 'ss.item.item_sk']: no join or merge connects the filter's source to any output-producing source. Add a join/merge relating them, or select a concept from the filter's model.
  ```
- `trilogy file write probe9.preql --run-and-delete`

  ```text
  Resolution error in probe9.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cr.catalog_returns_at_cr_item_item_sk_cr_order_number_join_cr.item.item_at_cr_item_item_sk_at_cr_item_item_sk_cr_order_number_grouped_by_cr.item.item_sk_at_cr_item_item_sk_at_cr_item_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cr_agg_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cs_agg_item_sk_grouped_by_cr_agg.item_sk_local._qual_items_item_sk_at_cr_agg_item_sk_local__qual_items_item_sk_filtered_by_6328308810022306_at_qual_items_item_sk_grouped_by_qual_items.item_sk_at_qual_items_item_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_6161156171680162_grouped_by_local.pos_city_local.pos_sn_local.pos_st_local.pos_zip_ss.coupon_amt_ss.date_dim.year_ss.item.color_ss.item.current_price_ss.item.product_name_ss.list_price_ss.store.store_name_ss.store.zip_ss.ticket_number_ss.wholesale_cost_at_local_pos_city_local_pos_sn_local_pos_st_local_pos_zip_ss_coupon_amt_ss_date_dim_year_ss_item_color_ss_item_current_price_ss_item_product_name_ss_list_price_ss_store_store_name_ss_store_zip_ss_ticket_number_ss_wholesale_cost onto cr.catalog_returns_at_cr_item_item_sk_cr_order_number_join_cr.item.item_at_cr_item_item_sk_at_cr_item_item_sk_cr_order_number_grouped_by_cr.item.item_sk_at_cr_item_item_sk_at_cr_item_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cr_agg_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cs_agg_item_sk_grouped_by_cr_agg.item_sk_local._qual_items_item_sk_at_cr_agg_item_sk_local__qual_items_item_sk_filtered_by_6328308810022306_at_qual_items_item_sk_grouped_by_qual_items.item_sk_at_qual_items_item_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_6161156171680162_grouped_by_local.cur_zip_at_local_cur_zip, cr.catalog_returns_at_cr_item_item_sk_cr_order_number_join_cr.item.item_at_cr_item_item_sk_at_cr_item_item_sk_cr_order_number_grouped_by_cr.item.item_sk_at_cr_item_item_sk_at_cr_item_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cr_agg_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cs_agg_item_sk_grouped_by_cr_agg.item_sk_local._qual_items_item_sk_at_cr_agg_item_sk_local__qual_items_item_sk_filtered_by_6328308810022306_at_qual_items_item_sk_grouped_by_qual_items.item_sk_at_qual_items_item_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_6161156171680162_grouped_by_local.fs_year_at_local_fs_year, cr.catalog_returns_at_cr_item_item_sk_cr_order_number_join_cr.item.item_at_cr_item_item_sk_at_cr_item_item_sk_cr_order_number_grouped_by_cr.item.item_sk_at_cr_item_item_sk_at_cr_item_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cr_agg_item_sk_at_cr_agg_item_sk_join_cs.catalog_sales_at_cs_item_item_sk_cs_order_number_grouped_by_cs.item.item_sk_at_cs_item_item_sk_at_cs_item_item_sk_at_cs_agg_item_sk_at_cs_agg_item_sk_grouped_by_cr_agg.item_sk_local._qual_items_item_sk_at_cr_agg_item_sk_local__qual_items_item_sk_filtered_by_6328308810022306_at_qual_items_item_sk_grouped_by_qual_items.item_sk_at_qual_items_item_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_6161156171680162_grouped_by_local.fsh_year_at_local_fsh_year. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe10.preql --run-and-delete`

  ```text
  Resolution error in probe10.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_7580522685662877_grouped_by_local.pos_city_local.pos_sn_local.pos_st_local.pos_zip_ss.coupon_amt_ss.date_dim.year_ss.item.color_ss.item.current_price_ss.item.product_name_ss.list_price_ss.store.store_name_ss.store.zip_ss.ticket_number_ss.wholesale_cost_at_local_pos_city_local_pos_sn_local_pos_st_local_pos_zip_ss_coupon_amt_ss_date_dim_year_ss_item_color_ss_item_current_price_ss_item_product_name_ss_list_price_ss_store_store_name_ss_store_zip_ss_ticket_number_ss_wholesale_cost onto sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_7580522685662877_grouped_by_local.cur_city_local.cur_sn_local.cur_st_local.cur_zip_at_local_cur_city_local_cur_sn_local_cur_st_local_cur_zip, sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_7580522685662877_grouped_by_local.fs_year_at_local_fs_year, sr.store_returns_at_sr_item_item_sk_sr_ticket_number_join_ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_join_ss.customer.customer_at_ss_customer_customer_sk_join_ss.customer.customer_demographics.customer_demographics_at_ss_customer_customer_demographics_demo_sk_join_ss.customer.first_sales_date.date_dim_at_ss_customer_first_sales_date_date_sk_join_ss.customer.first_shipto_date.date_dim_at_ss_customer_first_shipto_date_date_sk_join_ss.customer_address.customer_address_at_ss_customer_address_address_sk_join_ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_join_ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.item.item_at_ss_item_item_sk_join_ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_filtered_by_8582684690020660_at_ss_item_item_sk_ss_ticket_number_filtered_by_7580522685662877_grouped_by_local.fsh_year_at_local_fsh_year. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_grouped_by_ss.date_dim.year_at_ss_date_dim_year_at_ss_date_dim_year onto ss.store.store_at_ss_store_store_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probeM.preql --run-and-delete`

  ```text
  Syntax error in probeM.preql: Impossible comparison in ref:local.cat_key = ~: '~' can never match a declared value of enum<'Books', 'Children', 'Electronics', 'Home', 'Jewelry', 'Men', 'Music', 'Shoes', 'Sports', 'Women'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:ss.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.customer_sk', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.customer_sk order by ss.customer.customer_sk asc`.
  ```
- `trilogy file write probe_paths.preql --run-and-delete`

  ```text
  Resolution error in probe_paths.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk onto ss.date_dim.date_dim_at_ss_date_dim_date_sk_filtered_by_6231454891436400_grouped_by_ss.date_dim.date_at_ss_date_dim_date_at_ss_date_dim_date, ss.store.store_at_ss_store_store_sk_grouped_by_ss.store.store_id_at_ss_store_store_id_at_ss_store_store_id. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_426809370.preql --run`

  ```text
  Resolution error in answer_426809370.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cr.returning_customer.customer_address.customer_address_at_cr_returning_customer_customer_address_address_sk_filtered_by_9940674871789163_grouped_by_cr.returning_customer.customer_address.city_cr.returning_customer.customer_address.country_cr.returning_customer.customer_address.county_cr.returning_customer.customer_address.gmt_offset_cr.returning_customer.customer_address.location_type_cr.returning_customer.customer_address.state_cr.returning_customer.customer_address.street_name_cr.returning_customer.customer_address.street_number_cr.returning_customer.customer_address.street_type_cr.returning_customer.customer_address.suite_number_cr.returning_customer.customer_address.zip_at_cr_returning_customer_customer_address_city_cr_returning_customer_customer_address_country_cr_returning_customer_customer_address_county_cr_returning_customer_customer_address_gmt_offset_cr_returning_customer_customer_address_location_type_cr_returning_customer_customer_address_state_cr_returning_customer_customer_address_street_name_cr_returning_customer_customer_address_street_number_cr_returning_customer_customer_address_street_type_cr_returning_customer_customer_address_suite_number_cr_returning_customer_customer_address_zip_at_cr_returning_customer_customer_address_city_cr_returning_customer_customer_address_country_cr_returning_customer_customer_address_county_cr_returning_customer_customer_address_gmt_offset_cr_returning_customer_customer_address_location_type_cr_returning_customer_customer_address_state_cr_returning_customer_customer_address_street_name_cr_returning_customer_customer_address_street_number_cr_returning_customer_customer_address_street_type_cr_returning_customer_customer_address_suite_number_cr_returning_customer_customer_address_zip onto cr.catalog_returns_at_cr_item_item_sk_cr_order_number_join_cr.date_dim.date_dim_at_cr_date_dim_date_sk_join_cr.returning_addr.customer_address_at_cr_returning_addr_address_sk_join_cr.returning_customer.customer_at_cr_returning_customer_customer_sk_at_cr_item_item_sk_cr_order_number_grouped_by_cr.returning_addr.state_cr.returning_customer.customer_sk_at_cr_returning_addr_state_cr_returning_customer_customer_sk_join_cr.catalog_returns_at_cr_item_item_sk_cr_order_number_join_cr.date_dim.date_dim_at_cr_date_dim_date_sk_join_cr.returning_addr.customer_address_at_cr_returning_addr_address_sk_join_cr.returning_customer.customer_at_cr_returning_customer_customer_sk_at_cr_item_item_sk_cr_order_number_grouped_by_cr.returning_addr.state_cr.returning_customer.customer_sk_at_cr_returning_addr_state_cr_returning_customer_customer_sk_grouped_by_cr.returning_addr.state_at_cr_returning_addr_state_at_cr_returning_addr_state_cr_returning_customer_customer_sk_at_cr_returning_addr_state_cr_returning_customer_customer_sk_at_cr_returning_addr_state_cr_returning_customer_customer_sk, cr.returning_customer.customer_at_cr_returning_customer_customer_sk_at_cr_returning_customer_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.return_quantity_at_sr_return_quantity_at_sr_return_quantity onto sr.item.item_at_sr_item_item_sk_at_sr_item_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_sr.preql --run-and-delete`

  ```text
  Resolution error in probe_sr.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_sr_item_item_sk onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_sr_customer_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_sr2.preql --run-and-delete`

  ```text
  Resolution error in probe_sr2.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.ticket_number_at_sr_ticket_number_at_sr_ticket_number onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_sr_customer_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Resolution error in probe_a.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cu.household_demographics.income_band.income_band_at_cu_household_demographics_income_band_income_band_sk_filtered_by_9798578412816940_at_cu_household_demographics_income_band_income_band_sk onto cu.customer_at_cu_customer_sk_grouped_by_cu.customer_id_cu.household_demographics.demo_sk_at_cu_customer_id_cu_household_demographics_demo_sk_at_cu_customer_id_cu_household_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_sr4.preql --run-and-delete`

  ```text
  Resolution error in probe_sr4.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_sr_item_item_sk onto sr.customer.customer_demographics.customer_demographics_at_sr_customer_customer_demographics_demo_sk_at_sr_customer_customer_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_sr_cust.preql --run-and-delete`

  ```text
  Resolution error in probe_sr_cust.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_sr_item_item_sk onto sr.customer.customer_at_sr_customer_customer_sk_grouped_by_sr.customer.customer_id_at_sr_customer_customer_id_at_sr_customer_customer_id. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_f4.preql --run-and-delete`

  ```text
  Resolution error in probe_f4.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cu.household_demographics.income_band.income_band_at_cu_household_demographics_income_band_income_band_sk_at_cu_household_demographics_income_band_income_band_sk onto cu.customer_at_cu_customer_sk_filtered_by_145137781600581_grouped_by_cu.customer_id_cu.household_demographics.demo_sk_at_cu_customer_id_cu_household_demographics_demo_sk_at_cu_customer_id_cu_household_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_ss.preql --run-and-delete`

  ```text
  Resolution error in probe_ss.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk onto ss.customer.customer_at_ss_customer_customer_sk_grouped_by_ss.customer.customer_id_at_ss_customer_customer_id_at_ss_customer_customer_id, ss.customer_demographics.customer_demographics_at_ss_customer_demographics_demo_sk_at_ss_customer_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy explore root/store_returns.preql --show`

  ```text
  Option '--show' requires an argument.
  ```
- `trilogy file write probe_ib2.preql --run-and-delete`

  ```text
  Resolution error in probe_ib2.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cu.household_demographics.income_band.income_band_at_cu_household_demographics_income_band_income_band_sk_grouped_by_cu.household_demographics.income_band.lower_bound_cu.household_demographics.income_band.upper_bound_at_cu_household_demographics_income_band_lower_bound_cu_household_demographics_income_band_upper_bound_at_cu_household_demographics_income_band_lower_bound_cu_household_demographics_income_band_upper_bound onto cu.customer_at_cu_customer_sk_join_cu.household_demographics.household_demographics_at_cu_household_demographics_demo_sk_at_cu_customer_sk_at_cu_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_p1.preql --run-and-delete`

  ```text
  Resolution error in probe_p1.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.customer.customer_at_sr_customer_customer_sk_grouped_by_sr.customer.customer_id_at_sr_customer_customer_id_at_sr_customer_customer_id onto sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.return_amt_sr.ticket_number_at_sr_return_amt_sr_ticket_number_at_sr_return_amt_sr_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_p4.preql --run-and-delete`

  ```text
  Resolution error in probe_p4.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: hd.income_band.income_band_at_hd_income_band_income_band_sk_grouped_by_hd.income_band.lower_bound_hd.income_band.upper_bound_at_hd_income_band_lower_bound_hd_income_band_upper_bound_at_hd_income_band_lower_bound_hd_income_band_upper_bound onto hd.household_demographics_at_hd_demo_sk_at_hd_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_s6.preql --run-and-delete`

  ```text
  Resolution error in probe_s6.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.customer.customer_demographics.customer_demographics_at_sr_customer_customer_demographics_demo_sk_at_sr_customer_customer_demographics_demo_sk onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_join_sr.store_returns_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number_at_sr_item_item_sk_sr_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_r4a.preql --run-and-delete`

  ```text
  Resolution error in probe_r4a.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_sr_item_item_sk onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_sr_customer_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_r4b.preql --run-and-delete`

  ```text
  Resolution error in probe_r4b.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_sr_item_item_sk onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_sr_customer_demographics_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```

### `disabled-tool`

- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe store`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe item`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe store_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe household_demographics`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe customer`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read root/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write probe23.preql --run-and-delete`

  ```text
  refused to write 'probe23.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | union join ws2.ord = wr.order_number
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...e
     and ws2.sid is not null
   ??? union join ws2.ord = wr.order_...
  ```
- `trilogy file write answer_765177085.preql --run`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...return_quantity) as sr_cv,
     ??? subset join matched_states.cus...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...(sr.ticket_number) as cnt,
     ??? subset join matched.customer_s...
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...(sr.ticket_number) as cnt,
     ??? subset join matched.customer_s...
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  refused to write 'probe4.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...t(sr.ticket_number) as cnt
     ??? subset join matched.customer_s...
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  refused to write 'probe8.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:1
     |
  27 | select2 ss.customer.customer_sk as ck2, ss.item.item_sk as ik2, count(ss.ticket_number) as ss_rows2
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   and ss.date_dim.year = 2001
   ??? select2 ss.customer.customer_s...
  ```
- `trilogy file write probe16.preql --run-and-delete`

  ```text
  refused to write 'probe16.preql': not syntactically valid Trilogy.

  Parse error:
    --> 24:1
     |
  24 | select2 1
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   and ss.date_dim.year = 2001
   ??? select2 1
   limit 100;
  ```
- `trilogy file write probe18.preql --run-and-delete`

  ```text
  refused to write 'probe18.preql': not syntactically valid Trilogy.

  Parse error:
    --> 26:1
     |
  26 | subset join matched_r.ck = ss.customer.customer_sk and matched_r.ik = ss.item.item_sk
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...and matched_r.ck is not null
   ??? subset join matched_r.ck = ss....
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  refused to write 'probe1.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:1
     |
  23 | subset join sr.customer.customer_sk = ss.customer.customer_sk
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...omer.customer_sk is not null
   ??? subset join sr.customer.custom...
  ```
- `trilogy file write answer_751385098.preql --run`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...t count(sale_date) as combos
   ??? from intersect(
       (where ss...
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  refused to write 'probe_check.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:11
    |
  5 | with s as (where ss.customer.customer_sk is not null and ss.date_dim.year = 2000
    |           ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...web_sales as ws;

   with s as ??? (where ss.customer.customer_sk...
  ```
- `trilogy file write answer_1965638525.preql --run`

  ```text
  refused to write 'answer_1965638525.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:44
    |
  8 |     and ss.customer.customer_address.city <> ss.customer_address.city
    |                                            ^---
    |
    = expected sum_operator
  Location:
  ...stomer.customer_address.city < ??? > ss.customer_address.city
   se...
  ```
- `trilogy file write probe_null.preql --run-and-delete`

  ```text
  refused to write 'probe_null.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:1
     |
  12 | select ss.item.item_id
     | ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...is null) as null_price_sales
   ??? select ss.item.item_id
   where
  ```
- `trilogy file write probe_colors.preql --run-and-delete`

  ```text
  refused to write 'probe_colors.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:1
    |
  8 | select i.color;
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...e', 'blanched', 'burnished')
   ??? select i.color;

   # 2) How ma...
  ```
- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:45
    |
  5 |     (cs.bill_hdemo.buy_potential = '>10000' and cs.bill_cdemo.marital_status = 'D') as bill_direct_qual,
    |                                             ^---
    |
    = expected dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...demo.buy_potential = '>10000' ??? and cs.bill_cdemo.marital_stat...
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
    --> 14:1
     |
  14 | grouping...
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...omer.customer_sk is not null
   ??? grouping...
  ```
- `trilogy file write probe_st.preql --run-and-delete`

  ```text
  refused to write 'probe_st.preql': not syntactically valid Trilogy.

  Parse error:
   --> 3:12
    |
  3 | with st as (
    |            ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...re_sales as ss;

   with st as ??? (
       where ss.date_dim.date
  ```
- `trilogy file write answer_840315271.preql --run`

  ```text
  refused to write 'answer_840315271.preql': not syntactically valid Trilogy.

  Parse error:
    --> 19:14
     |
  19 |     average <> 0
     |              ^---
     |
     = expected sum_operator
  Location:

       )
   having
       average < ??? > 0
       and abs(total - avera...
  ```
- `trilogy file write answer_2822608529.preql --run`

  ```text
  refused to write 'answer_2822608529.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | where ws.ship_hdemo.dep_count = 6
    | ^---
    |
    = expected IDENTIFIER
  Location:
  ...oot.web_sales as ws;

   with
   ??? where ws.ship_hdemo.dep_count
  ```
- `trilogy file write probe_distinct.preql --run-and-delete`

  ```text
  refused to write 'probe_distinct.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:12
    |
  4 | with sc as (
    |            ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...og_sales as cs;

   with sc as ??? (
       where ss.date_dim.year
  ```

### `undefined-concept`

- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Undefined concept: wr.web_site.site_id. Suggestions: ['wr.item.item_id', 'wr.item.item_desc', 'wr.item.item_sk', 'site_id']
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Undefined concept: wr.site_id. Suggestions: ['wr.item.item_id', 'wr.item.class_id', 'wr.item.brand_id', 'site_id']
  ```
- `trilogy file write probe23.preql --run-and-delete`

  ```text
  Syntax error in probe23.preql: Undefined concept: ws2.ord. Suggestions: ['ws2.sid', 'order_site.ord']
  ```
- `trilogy file write probe_G.preql --run-and-delete`

  ```text
  Syntax error in probe_G.preql: Undefined concept: item.item_sk. Suggestions: ['ss.item.item_sk', 'sr.item.item_sk', 'cs.item.item_sk', 'ss.promotion.item.item_sk', 'cs.promotion.item.item_sk', 'item.item_desc']
  ```
- `trilogy file write probe_K.preql --run-and-delete`

  ```text
  Syntax error in probe_K.preql: Undefined concept: matched. Suggestions: ['matched.ticket', 'matched.item_sk']
  ```
- `trilogy file write answer_3840922834.preql --run`

  ```text
  Syntax error in answer_3840922834.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ws.sold_date.year', 'ws.ship_date.year', 'ws.promotion.end_date.year']
  ```

### `join-resolution`

- `trilogy file write probe_years2.preql --run-and-delete`

  ```text
  Resolution error in probe_years2.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cat_rows}; {web_rows, year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_pref_zips_zip}; {_virt_agg_count_2017297982338284}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {cs.order_number, cs_cust, cs_qty, cs_year}; {sr_cust, sr_ticket, sr_year, sr.return_quantity}; {ss_year, ss.customer.customer_sk, ss.date_dim.year, ss.item.item_desc, ss.item.item_id, ss.item.item_sk, ss.quantity, ss.store.state, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write answer_2604809012.preql --run`

  ```text
  Resolution error in answer_2604809012.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 4 disconnected subgraphs: {d.date}; {i.current_price, item_code}; {after_total, before_total}; {warehouse_name}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write answer_145690531.preql --run`

  ```text
  Resolution error in answer_145690531.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_subquery_6_53.d.week_seq}; {_store_totals_item_id, _store_totals_store_total, ss.date_dim.week_seq}. Are you missing a join or merge statement to relate them?
  ```

### `type-error`

- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy file write answer_2374450308.preql --run`

  ```text
  Syntax error in answer_2374450308.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```

### `cli-misuse`

- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/store_returns.preql --show datasource`

  ```text
  Invalid value for '--show': 'datasource' is not one of 'all', 'concepts', 'datasources', 'imports', 'groups'.
  ```

### `syntax-missing-alias`

- `trilogy file write probe7.preql --run-and-delete`

  ```text
  refused to write 'probe7.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `cs.bill_hdemo.buy_potential = '>10000' and cs.bill_cdemo.marital_status = 'D' as cs_bill_hdemo_buy_potential_10000_and_cs`
  Location:
  ...demo.buy_potential = '>10000' ??? and cs.bill_cdemo.marital_stat...
  ```
