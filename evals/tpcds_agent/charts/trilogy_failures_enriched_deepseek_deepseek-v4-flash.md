# Trilogy failure analysis — 20260817-163555

- Run `20260817-163552_enriched_deepseek_deepseek-v4-flash_docstrim` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1197 | failed: 69 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 28 | 41% |
| `disabled-tool` | 15 | 22% |
| `syntax-parse` | 7 | 10% |
| `undefined-concept` | 7 | 10% |
| `cli-misuse` | 4 | 6% |
| `planner-recursion` | 4 | 6% |
| `join-resolution` | 2 | 3% |
| `type-error` | 2 | 3% |

## Detail

### `other`

- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: a.return_date.date_at_a_return_date_sk_filtered_by_6880519126965076_grouped_by_a.return_date.date_at_a_return_date_date_at_local_rdate onto a.store_dim_return_unified_at_a_channel_a_return_channel_dim_id_join_a.store_dim_unified_at_a_channel_a_channel_dim_id_join_a.store_returns_unified_at_a_channel_a_item_sk_a_order_id_join_a.store_sales_unified_at_a_channel_a_item_sk_a_order_id_at_a_channel_a_item_sk_a_order_id_filtered_by_8794791481918953_at_a_channel_a_item_sk_a_order_id. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe4.preql --run-and-delete`

  ```text
  Resolution error in probe4.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: a.return_date.date_at_a_return_date_sk_filtered_by_6880519126965076_grouped_by_a.return_date.date_at_a_return_date_date_at_local_rdate onto a.catalog_dim_return_unified_at_a_channel_a_return_channel_dim_id_join_a.catalog_dim_unified_at_a_channel_a_channel_dim_id_join_a.catalog_returns_unified_at_a_channel_a_item_sk_a_order_id_join_a.catalog_sales_unified_at_a_channel_a_item_sk_a_order_id_at_a_channel_a_item_sk_a_order_id_filtered_by_961928751419228_at_a_channel_a_item_sk_a_order_id. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe3a.preql --run-and-delete`

  ```text
  Resolution error in probe3a.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.return_date.date_at_ss_return_date_sk_filtered_by_2854436740443212_grouped_by_ss.return_date.date_at_ss_return_date_date_at_local_rdate onto ss.store_returns_at_ss_item_sk_ss_ticket_number_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Resolution error in probe5.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: a.sale_date.date_at_a_sale_date_sk_filtered_by_5872866479754658_grouped_by_a.sale_date.date_at_a_sale_date_date_at_local_sdate onto a.catalog_dim_unified_at_a_channel_a_channel_dim_id_union_a.store_dim_unified_at_a_channel_a_channel_dim_id_union_a.web_dim_unified_at_a_channel_a_channel_dim_id_unioned_join_a.catalog_sales_unified_at_a_channel_a_item_sk_a_order_id_union_a.store_sales_unified_at_a_channel_a_item_sk_a_order_id_union_a.web_sales_unified_at_a_channel_a_item_sk_a_order_id_unioned_at_a_channel_a_item_sk_a_order_id_at_a_channel_a_item_sk_a_order_id. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe6.preql --run-and-delete`

  ```text
  Resolution error in probe6.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: a.return_date.date_at_a_return_date_sk_filtered_by_1235177457898933_grouped_by_a.return_date.date_at_a_return_date_date_at_local_rdate onto a.catalog_dim_return_unified_at_a_channel_a_return_channel_dim_id_union_a.store_dim_return_unified_at_a_channel_a_return_channel_dim_id_union_a.web_dim_return_unified_at_a_channel_a_return_channel_dim_id_unioned_join_a.catalog_returns_unified_at_a_channel_a_item_sk_a_order_id_union_a.store_returns_unified_at_a_channel_a_item_sk_a_order_id_union_a.web_returns_unified_at_a_channel_a_item_sk_a_order_id_unioned_join_a.catalog_returns_unified_at_a_channel_a_item_sk_a_order_id_union_a.store_returns_unified_at_a_channel_a_item_sk_a_order_id_union_a.web_sales_unified_at_a_channel_a_item_sk_a_order_id_unioned_join_a.catalog_sales_unified_at_a_channel_a_item_sk_a_order_id_union_a.store_sales_unified_at_a_channel_a_item_sk_a_order_id_union_a.web_sales_unified_at_a_channel_a_item_sk_a_order_id_unioned_at_a_channel_a_item_sk_a_order_id_at_a_channel_a_item_sk_a_order_id. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Syntax error in probe_a.preql: This script requires parameter "zips" to be set in environment.
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Syntax error in probe_b.preql: This script requires parameter "zips" to be set in environment.
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Syntax error in probe_check.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 13). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Resolution error in probe_check.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.order_number_at_cs_order_number_at_local__base_filters_order_number onto cs.call_center.call_center_at_cs_call_center_sk_filtered_by_493536125453931_grouped_by_cs.call_center.county_at_cs_call_center_county_at_local__base_filters_cc. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_join4.preql --run-and-delete`

  ```text
  [v4] group-graph lineage cycle, skipping concept-set pass: [('grp:rowset:d0:catalog_side.cust_sk|catalog_side.item_sk|store_side.cust_sk|store_side.item_desc|store_side.item_id|store_side.item_sk|store_side.sr_qty|store_side.ss_qty|store_side.st|store_side.ticket:rowset:store_side', 'grp:rowset:d0:catalog_side.cust_sk|catalog_side.item_sk|catalog_side.order_no:rowset:catalog_side'), ('grp:rowset:d0:catalog_side.cust_sk|catalog_side.item_sk|catalog_side.order_no:rowset:catalog_side', 'grp:rowset:d0:catalog_side.cust_sk|catalog_side.item_sk|store_side.cust_sk|store_side.item_desc|store_side.ite
  …
  em_sk|store_side.cust_sk|store_side.item_desc|store_side.item_id|store_side.item_sk|store_side.sr_qty|store_side.ss_qty|store_side.st|store_side.ticket:rowset:store_side', 'grp:rowset:d0:catalog_side.cust_sk|catalog_side.item_sk|catalog_side.order_no:rowset:catalog_side'), ('grp:rowset:d0:catalog_side.cust_sk|catalog_side.item_sk|catalog_side.order_no:rowset:catalog_side', 'grp:rowset:d0:catalog_side.cust_sk|catalog_side.item_sk|store_side.cust_sk|store_side.item_desc|store_side.item_id|store_side.item_sk|store_side.sr_qty|store_side.ss_qty|store_side.st|store_side.ticket:rowset:store_side')]
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_grouped_by_ss.customer.current_address.county_ss.customer.current_address.state_at_ss_customer_current_address_county_ss_customer_current_address_state onto cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_9131557179469134_at_cs_item_sk_local____tvf_arm_0_cid_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_local__qualifying_customers_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_713947544960943_at_local____tvf_arm_1_cid_ws_item_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_local__qualifying_customers_cid_unioned_at_qualifying_customers_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store.store_at_ss_store_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_3254463733854215_at_ss_item_sk_ss_sale_date_month_seq_ss_store_county_ss_store_state_ss_ticket_number_filtered_by_9911583417019843, cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_9131557179469134_at_cs_item_sk_local____tvf_arm_0_cid_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_local__qualifying_customers_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_713947544960943_at_local____tvf_arm_1_cid_ws_item_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_local__qualifying_customers_cid_unioned_at_qualifying_customers_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_3254463733854215_at_ss_item_sk_ss_sale_date_month_seq_ss_ticket_number_filtered_by_9911583417019843_join_d.date_at_d_sk_grouped_by__at_abstract_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_sale_date_month_seq_ss_ticket_number_filtered_by_6060002969759691_grouped_by_ss.customer.sk_at_ss_customer_sk_at_ss_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_3553309440.preql --run`

  ```text
  Resolution error in answer_3553309440.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.customer.current_address.customer_address_at_ss_customer_current_address_sk_grouped_by_ss.customer.current_address.county_ss.customer.current_address.state_at_ss_customer_current_address_county_ss_customer_current_address_state onto cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_9131557179469134_at_cs_item_sk_local____tvf_arm_0_cid_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_local__qualifying_customers_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_713947544960943_at_local____tvf_arm_1_cid_ws_item_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_local__qualifying_customers_cid_unioned_at_qualifying_customers_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store.store_at_ss_store_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_3254463733854215_at_ss_item_sk_ss_sale_date_month_seq_ss_store_county_ss_store_state_ss_ticket_number_filtered_by_9911583417019843, cs.catalog_sales_at_cs_item_sk_cs_order_number_grouped_by_cs.billing_customer.sk_cs.item.sk_cs.sale_date.sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_join_cs.item.items_at_cs_item_sk_join_cs.sale_date.date_at_cs_sale_date_sk_at_cs_billing_customer_sk_cs_item_sk_cs_sale_date_sk_filtered_by_9131557179469134_at_cs_item_sk_local____tvf_arm_0_cid_grouped_by_local.___tvf_arm_0_cid_at_local____tvf_arm_0_cid_at_local____tvf_arm_0_cid_local__qualifying_customers_cid_union_ws.item.items_at_ws_item_sk_join_ws.sale_date.date_at_ws_sale_date_sk_join_ws.web_sales_at_ws_item_sk_ws_order_number_grouped_by_ws.billing_customer.sk_ws.item.sk_ws.sale_date.sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_at_ws_billing_customer_sk_ws_item_sk_ws_sale_date_sk_filtered_by_713947544960943_at_local____tvf_arm_1_cid_ws_item_sk_grouped_by_local.___tvf_arm_1_cid_at_local____tvf_arm_1_cid_at_local____tvf_arm_1_cid_local__qualifying_customers_cid_unioned_at_qualifying_customers_cid_join_ss.sale_date.date_at_ss_sale_date_sk_join_ss.store_sales_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_ticket_number_filtered_by_3254463733854215_at_ss_item_sk_ss_sale_date_month_seq_ss_ticket_number_filtered_by_9911583417019843_join_ss.sale_date.date_at_ss_sale_date_sk_grouped_by__at_abstract_at_ss_item_sk_ss_ticket_number_at_ss_item_sk_ss_sale_date_month_seq_ss_ticket_number_filtered_by_6060002969759691_grouped_by_ss.customer.sk_at_ss_customer_sk_at_ss_customer_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_week.preql --run-and-delete`

  ```text
  Syntax error in probe_week.preql: Cannot compare DATE (ref:d.date) and STRING (2000-01-03) of different types with operator = in ref:d.date = 2000-01-03
  ```
- `trilogy file write answer_4037365554.preql --run`

  ```text
  Syntax error in answer_4037365554.preql: Output column 'monthly_total' renames 'local.monthly_total' back to the name of an existing concept 'monthly_total' (defined at line 3) that 'local.monthly_total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'monthly_total_out').
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: ORDER BY references 'ss.customer.id', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.customer.id order by ss.customer.id asc`.
  ```
- `trilogy file write answer_426809370.preql --run`

  ```text
  Resolution error in answer_426809370.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.return_customer.current_address.customer_address_at_cs_return_customer_current_address_sk_filtered_by_5987495888938297_grouped_by_cs.return_customer.current_address.city_cs.return_customer.current_address.country_cs.return_customer.current_address.county_cs.return_customer.current_address.gmt_offset_cs.return_customer.current_address.location_type_cs.return_customer.current_address.state_cs.return_customer.current_address.street_name_cs.return_customer.current_address.street_number_cs.return_customer.current_address.street_type_cs.return_customer.current_address.suite_number_cs.return_customer.current_address.zip_at_cs_return_customer_current_address_city_cs_return_customer_current_address_country_cs_return_customer_current_address_county_cs_return_customer_current_address_gmt_offset_cs_return_customer_current_address_location_type_cs_return_customer_current_address_state_cs_return_customer_current_address_street_name_cs_return_customer_current_address_street_number_cs_return_customer_current_address_street_type_cs_return_customer_current_address_suite_number_cs_return_customer_current_address_zip_at_local_city_local_country_local_county_local_gmt_offset_local_location_type_local_state_local_street_name_local_street_number_local_street_type_local_suite_number_local_zip onto cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_join_cs.catalog_returns_at_cs_item_sk_cs_order_number_join_cs.catalog_sales_at_cs_item_sk_cs_order_number_join_cs.return_address.customer_address_at_cs_return_address_sk_join_cs.return_date.date_at_cs_return_date_sk_at_cs_item_sk_cs_order_number_grouped_by_cs.return_address.state_cs.return_customer.sk_at_cs_return_address_state_cs_return_customer_sk_grouped_by_cs.return_address.state_at_cs_return_address_state_at_cs_return_address_state_cs_return_customer_sk_at_cs_return_address_state_cs_return_customer_sk_at_cs_return_address_state_cs_return_customer_sk, cs.return_customer.customers_at_cs_return_customer_sk_at_cs_return_customer_sk_local_customer_code. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Syntax error in probe8.preql: Output column 'state_avg' renames 'local.state_avg' back to the name of an existing concept 'state_avg' (defined at line 12) that 'local.state_avg' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'state_avg_out').
  ```
- `trilogy file write probe11.preql --run-and-delete`

  ```text
  Unexpected error in probe11.preql: Invalid input concepts to node! ['base.state'] are missing non-hidden parent nodes; have {'local.ratio', 'local.state_avg_out', 'base.total', 'stavg.avg_total', 'base.ret_state', 'local.customer_code', 'base.customer_code', 'local.total', 'local.ret_state'} and hidden {'base.cust_sk', 'base.ret_state', 'base.customer_code', 'stavg.ret_state', 'base.state'} from root {'local.ratio', 'local.state_avg_out', 'local.customer_code', 'local.total', 'local.ret_state'}
  ```
- `trilogy file write answer_2869182220.preql --run`

  ```text
  Resolution error in answer_2869182220.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.return_customer_demographic.customer_demographics_at_ss_return_customer_demographic_sk_at_local__ret_lines_ret_cdemo_sk onto ss.item.items_at_ss_item_sk_at_local__ret_lines_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_rs.preql --run-and-delete`

  ```text
  Resolution error in probe_rs.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.return_customer_demographic.customer_demographics_at_ss_return_customer_demographic_sk_at_local__ret_lines_ret_cdemo_sk onto ss.item.items_at_ss_item_sk_at_local__ret_lines_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Resolution error in probe_a.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.store_sales_at_ss_item_sk_ss_ticket_number_grouped_by_ss.ticket_number_at_ss_ticket_number_at_local__ret_lines_ticket_number onto ss.return_customer_demographic.customer_demographics_at_ss_return_customer_demographic_sk_at_local__ret_lines_ret_cdemo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Resolution error in probe_b.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.return_customer_demographic.customer_demographics_at_ss_return_customer_demographic_sk_at_local__ret_lines_ret_cdemo_sk onto ss.item.items_at_ss_item_sk_at_local__ret_lines_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_c.preql --run-and-delete`

  ```text
  Resolution error in probe_c.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.return_customer_demographic.customer_demographics_at_ss_return_customer_demographic_sk_at_local_ret_cdemo_sk onto ss.item.items_at_ss_item_sk_at_local_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_i.preql --run-and-delete`

  ```text
  Syntax error in probe_i.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 17). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe_k.preql --run-and-delete`

  ```text
  Syntax error in probe_k.preql: HAVING filters on a dimension outside the SELECT projection, but the select has no grain key to anchor a post-aggregation semijoin (line 12). Move the filter to WHERE to filter before aggregation.
  ```
- `trilogy file write probe_k.preql --run-and-delete`

  ```text
  Unexpected error in probe_k.preql: Could not render the query: Missing source reference to ss.ticket_number; Missing source reference to ss.item.sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  juicy as (
  SELECT
      "ss_store_returns"."SR_CDEMO_SK" as "ss_return_customer_demographic_sk",
      coalesce("ss_store_returns"."SR_ITEM_SK","ss_store_sales"."SS_ITEM_SK") as "ss_item_sk",
      coalesce("ss_store_returns"."SR_TICKET_NUMBER","ss_store_sales"."SS_TICKET_NUMBER") as "ss_ticket_number"
  FROM
      "store_sales" as "ss_store_sales"
      LEFT OUTER JOIN "store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"),
  vacuous as (
  SELECT
      "juicy"."ss_item_sk" as "_ret_lines_item_sk",
      "juicy"."ss_return_customer_demographic_sk" as "_ret_lines_ret_cdemo_sk",
      "juicy"."ss_ticket_number" as "_ret_lines_ticket_number"
  FROM
      "juicy"
  WHERE
      "juicy"."ss_return_customer_demographic_sk" is not null
  ),
  concerned as (
  SELECT
      "vacuous"."_ret_lines_item_sk" as "_ret_lines_item_sk",
      "vacuous"."_ret_lines_item_sk" as "ret_lines_item_sk",
      "vacuous"."_ret_lines_ret_cdemo_sk" as "ret_lines_ret_cdemo_sk",
      "vacuous"."_ret_lines_ticket_number" as "_ret_lines_ticket_number",
      "vacuous"."_ret_lines_ticket_number" as "ret_lines_ticket_number",
      1 as "__preql_internal_all_rows"
  FROM
      "vacuous"),
  thoughtful as (
  SELECT
      "ss_customer_customers"."C_CURRENT_CDEMO_SK" as "_edge_customers_cdemo_sk",
      "ss_customer_customers"."C_CUSTOMER_ID" as "_edge_customers_customer_code"
  FROM
      "customer" as "ss_customer_customers"
      INNER JOIN "customer_address" as "ss_customer_current_address_customer_address" on "ss_customer_customers"."C_CURRENT_ADDR_SK" = "ss_customer_current_address_customer_address"."CA_ADDRESS_SK"
      INNER JOIN "household_demographics" as "ss_customer_current_household_demographics_household_demographics" on "ss_customer_customers"."C_CURRENT_HDEMO_SK" = "ss_customer_current_household_demographics_household_demographics"."HD_DEMO_SK"
      INNER JOIN "income_band" as "ss_customer_current_household_demographics_income_band_income_band" on "ss_customer_current_household_demographics_household_demographics"."HD_INCOME_BAND_SK" = "ss_customer_current_household_demographics_income_band_income_band"."IB_INCOME_BAND_SK"
  WHERE
      "ss_customer_current_address_customer_address"."CA_CITY" = 'Edgewood' and "ss_customer_customers"."C_CURRENT_CDEMO_SK" is not null and "ss_customer_current_household_demographics_income_band_income_band"."IB_LOWER_BOUND" >= 38128 and "ss_customer_current_household_demographics_income_band_income_band"."IB_UPPER_BOUND" <= 88128
  ),
  questionable as (
  SELECT
      "thoughtful"."_edge_customers_cdemo_sk" as "edge_customers_cdemo_sk",
      "thoughtful"."_edge_customers_customer_code" as "edge_customers_customer_code",
      1 as "__preql_internal_all_rows"
  FROM
      "thoughtful"),
  young as (
  SELECT
      "concerned"."ret_lines_item_sk" as "ret_lines_item_sk",
      "concerned"."ret_lines_ticket_number" as "ret_lines_ticket_number",
      "questionable"."edge_customers_customer_code" as "edge_customers_customer_code"
  FROM
      "questionable"
      FULL JOIN "concerned" on "questionable"."__preql_internal_all_rows" = "concerned"."__preql_internal_all_rows" AND "questionable"."edge_customers_cdemo_sk" = "concerned"."ret_lines_ret_cdemo_sk"),
  late as (
  SELECT
      CASE WHEN "young"."edge_customers_customer_code" is not null THEN "young"."ret_lines_item_sk" ELSE NULL END as "_virt_filter_item_sk_2738055015715421",
      CASE WHEN "young"."edge_customers_customer_code" is not null THEN "young"."ret_lines_ticket_number" ELSE NULL END as "_virt_filter_ticket_number_657903382954535"
  FROM
      "young"),
  macho as (
  SELECT
      "late"."_virt_filter_item_sk_2738055015715421" as "_virt_filter_item_sk_2738055015715421",
      "late"."_virt_filter_ticket_number_657903382954535" as "_virt_filter_ticket_number_657903382954535"
  FROM
      "late"
  GROUP BY
      1,
      2),
  abhorrent as (
  SELECT
      "concerned"."ret_lines_ret_cdemo_sk" as "ret_lines_ret_cdemo_sk"
  FROM
      "concerned"
  GROUP BY
      1,
      "concerned"."__preql_internal_all_rows",
      "concerned"."_ret_lines_item_sk",
      "concerned"."_ret_lines_ticket_number"),
  sparkling as (
  SELECT
      count(md5(CONCAT_WS('', coalesce(cast("young"."edge_customers_customer_code" as string),'
  '), coalesce(cast("young"."ret_lines_ticket_number" as string),'
  '), coalesce(cast("young"."ret_lines_item_sk" as string),'
  ')))) as "n_combos"
  FROM
      "young"),
  abundant as (
  SELECT
      count("questionable"."edge_customers_customer_code") as "n_matched_customers"
  FROM
      "questionable"),
  sweltering as (
  SELECT
      "abundant"."n_matched_customers" as "n_matched_customers",
      "sparkling"."n_combos" as "n_combos"
  FROM
      "questionable"
      FULL JOIN "abhorrent" on "questionable"."edge_customers_cdemo_sk" = "abhorrent"."ret_lines_ret_cdemo_sk"
      RIGHT OUTER JOIN "sparkling" on 1=1
      INNER JOIN "abundant" on 1=1
  GROUP BY
      1,
      2)
  SELECT
      coalesce("sweltering"."n_matched_customers",0) as "n_matched_customers",
      coalesce("sweltering"."n_combos",0) as "n_combos"
  FROM
      "sweltering"
  WHERE
      INVALID_REFERENCE_BUG<Missing source reference to ss.ticket_number> is not null and exists (select 1 from macho where macho."_virt_filter_item_sk_2738055015715421" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to ss.item.sk> and macho."_virt_filter_ticket_number_657903382954535" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to ss.ticket_number>)
  ```
- `trilogy file write answer_2852230229.preql --run`

  ```text
  Syntax error in answer_2852230229.preql: ORDER BY references 'local.parent', which is not in the SELECT projection (line 11). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.parent order by local.parent asc`.
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Syntax error in probe_verify.preql: ORDER BY references 'ss.ticket_number', which is not in the SELECT projection (line 8). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --ss.ticket_number order by ss.ticket_number asc`.
  ```

### `disabled-tool`

- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/item.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

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
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/all_sales.preql`

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
- `trilogy file read raw/all_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy file write probe3.preql --run-and-delete`

  ```text
  refused to write 'probe3.preql': not syntactically valid Trilogy.

  Parse error:
   --> 8:30
    |
  8 |     sum(price_per_id > 1 ? 1 : 0) as ids_with_multiple_prices,
    |                              ^---
    |
    = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
       sum(price_per_id > 1 ? 1 ??? : 0) as ids_with_multiple_pric...
  ```
- `trilogy file write probe_var.preql --run-and-delete`

  ```text
  refused to write 'probe_var.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...nt(matched_lines.ss_qty) as n ??? from matched_lines;
  ```
- `trilogy file write answer_2133330107.preql --run`

  ```text
  refused to write 'answer_2133330107.preql': not syntactically valid Trilogy.

  Parse error:
    --> 13:59
     |
  13 |     and substring(ss.customer.current_address.zip, 1, 5) <> substring(ss.store.zip, 1, 5)
     |                                                           ^---
     |
     = expected sum_operator
  Location:
  ...r.current_address.zip, 1, 5) < ??? > substring(ss.store.zip, 1, 5...
  ```
- `trilogy file write answer_2844519538.preql --run`

  ```text
  refused to write 'answer_2844519538.preql': not syntactically valid Trilogy.

  Parse error:
    --> 11:46
     |
  11 |        and lower(ss.customer.birth_country) <> lower(ss.customer.current_address.country)))
     |                                              ^---
     |
     = expected sum_operator
  Location:
  ...r(ss.customer.birth_country) < ??? > lower(ss.customer.current_ad...
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  refused to write 'probe2.preql': not syntactically valid Trilogy.

  Parse error:
    --> 18:1
     |
  18 | union join ss.customer.sk = cs.billing_customer.sk
     | ^---
     |
     = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...e.year in (1999, 2000, 2001)
   ??? union join ss.customer.sk = cs...
  ```
- `trilogy file write probe_ids.preql --run-and-delete`

  ```text
  refused to write 'probe_ids.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...;

   select st.id as store_id ??? from st order by st.id limit 3...
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  refused to write 'probe_store.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...year is null) as null_month,
   ???
  ```

### `undefined-concept`

- `trilogy file write probe_c.preql --run-and-delete --param zips=24128,76232,65084`

  ```text
  Syntax error in probe_c.preql: Undefined concept: local.qualifying_prefix_placeholder (line 12, col 42, in WHERE). Suggestions: ['qualifying.prefix']
  ```
- `trilogy file write probe_rowset.preql --run-and-delete`

  ```text
  Syntax error in probe_rowset.preql: Undefined concept: s.cust_sk. Suggestions: ['s.item_sk', 'c.cust_sk', 'store_side.cust_sk', 'catalog_side.cust_sk', 'ss.customer.sk']
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: Undefined concept: _virt_agg_grouping_9769516219811737.
  ```
- `trilogy file write probe_2874833976.preql --run-and-delete`

  ```text
  Syntax error in probe_2874833976.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  Syntax error in answer_2874833976.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```
- `trilogy file write probe_2874833976.preql --run-and-delete`

  ```text
  Syntax error in probe_2874833976.preql: Undefined concept: _virt_agg_grouping_9769516219811737.
  ```
- `trilogy file write probe_2874833976.preql --run-and-delete`

  ```text
  Syntax error in probe_2874833976.preql: Undefined concept: _virt_agg_grouping_4453574046401692.
  ```

### `cli-misuse`

- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy file show raw/catalog_sales.preql`

  ```text
  No such command 'show'.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```
- `trilogy explore raw/orders.preql`

  ```text
  Invalid value for 'PATH': File 'raw/orders.preql' does not exist.
  ```

### `planner-recursion`

- `trilogy file write answer_765177085.preql --run`

  ```text
  Resolution error in answer_765177085.preql: query could not be planned; this is a bug.
  ```
- `trilogy file write answer_765177085.preql --run`

  ```text
  Resolution error in answer_765177085.preql: query could not be planned; this is a bug.
  ```
- `trilogy file write probe_final2.preql --run-and-delete`

  ```text
  Resolution error in probe_final2.preql: query could not be planned; this is a bug.
  ```
- `trilogy file write probe_cd.preql --run-and-delete`

  ```text
  Resolution error in probe_cd.preql: query could not be planned; this is a bug.
  ```

### `join-resolution`

- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Resolution error in probe5.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {benchmark_avg}; {lines_jan2001, ss.item.category, ss.sale_date.month_of_year, ss.sale_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_setops.preql --run-and-delete`

  ```text
  Resolution error in probe_setops.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_c}; {_catalog_only_i}. Are you missing a join or merge statement to relate them?
  ```

### `type-error`

- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Syntax error in probe1.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Syntax error in probe2.preql: Cannot use BETWEEN with incompatible types DATE and STRING (low)
  ```
