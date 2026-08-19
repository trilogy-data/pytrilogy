# Trilogy failure analysis — 20260817-163555

- Run `20260817-163552_ingest_deepseek_deepseek-v4-flash_docstrim` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 1302 | failed: 97 (7%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `disabled-tool` | 27 | 28% |
| `other` | 24 | 25% |
| `undefined-concept` | 21 | 22% |
| `join-resolution` | 9 | 9% |
| `syntax-parse` | 9 | 9% |
| `cli-misuse` | 6 | 6% |
| `no-output` | 1 | 1% |

## Detail

### `disabled-tool`

- `trilogy database describe web_returns`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read root/web_returns.preql`

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
- `trilogy file read root/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_returns.preql`

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
- `trilogy file read root/store.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

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
- `trilogy file read root/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/web_sales.preql`

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
- `trilogy file read root/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read root/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `other`

- `trilogy file write probe_webch.preql --run-and-delete`

  ```text
  Syntax error in probe_webch.preql: union(...) requires at least two relational arms.
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Syntax error in probe3.preql: This script requires parameter "zips" to be set in environment.
  ```
- `trilogy file write probe_check.preql --run-and-delete`

  ```text
  Unexpected error in probe_check.preql: Could not render the query: Missing source reference to ws.sold_date.year; Missing source reference to ws.sold_date.moy; Missing source reference to ws.bill_customer.customer_sk; Missing source reference to ss.customer.customer_sk; Missing source reference to cs.sold_date.year; Missing source reference to cs.sold_date.moy; Missing source reference to cs.ship_customer.customer_sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  wakeful as (
  SELECT
      CASE WHEN ( INVALID_REFERENCE_BUG<Missing source reference to ws.sold_date.year> = 2002 and (INVALID_REFERENCE_BUG<Missing source reference to ws.sold_date.moy> is not null and INVALID_REFERENCE_BUG<Missing source reference to ws.sold_date.moy> in (1,2,3,4)) ) THEN INVALID_REFERENCE_BUG<Missing source reference to ws.bill_customer.customer_sk> ELSE NULL END as "web_cust"
  ),
  thoughtful as (
  SELECT
      CASE WHEN exists (select 1 from wakeful where wakeful."web_cust" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk>) THEN INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk> ELSE NULL END as "_virt_filter_customer_sk_2834222161828100",
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk> as "ss_customer_customer_sk"
  FROM
      "wakeful"),
  quizzical as (
  SELECT
      CASE WHEN ( INVALID_REFERENCE_BUG<Missing source reference to cs.sold_date.year> = 2002 and (INVALID_REFERENCE_BUG<Missing source reference to cs.sold_date.moy> is not null and INVALID_REFERENCE_BUG<Missing source reference to cs.sold_date.moy> in (1,2,3,4)) ) THEN INVALID_REFERENCE_BUG<Missing source reference to cs.ship_customer.customer_sk> ELSE NULL END as "cat_cust"
  ),
  cheerful as (
  SELECT
      CASE WHEN ( exists (select 1 from wakeful where wakeful."web_cust" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk>) or exists (select 1 from quizzical where quizzical."cat_cust" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk>) ) THEN INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk> ELSE NULL END as "_virt_filter_customer_sk_569015855313349",
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk> as "ss_customer_customer_sk"
  FROM
      "wakeful"),
  highfalutin as (
  SELECT
      CASE WHEN exists (select 1 from quizzical where quizzical."cat_cust" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk>) THEN INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk> ELSE NULL END as "_virt_filter_customer_sk_7731278412084335",
      INVALID_REFERENCE_BUG<Missing source reference to ss.customer.customer_sk> as "ss_customer_customer_sk"
  FROM
      "quizzical"),
  cooperative as (
  SELECT
      "cheerful"."_virt_filter_customer_sk_569015855313349" as "_virt_filter_customer_sk_569015855313349",
      "highfalutin"."_virt_filter_customer_sk_7731278412084335" as "_virt_filter_customer_sk_7731278412084335",
      "highfalutin"."ss_customer_customer_sk" as "ss_customer_customer_sk",
      "thoughtful"."_virt_filter_customer_sk_2834222161828100" as "_virt_filter_customer_sk_2834222161828100"
  FROM
      "thoughtful"
      INNER JOIN "cheerful" on "thoughtful"."ss_customer_customer_sk" = "cheerful"."ss_customer_customer_sk"
      INNER JOIN "highfalutin" on "thoughtful"."ss_customer_customer_sk" = "highfalutin"."ss_customer_customer_sk"
  GROUP BY
      1,
      2,
      3,
      4)
  SELECT
      count(distinct "cooperative"."ss_customer_customer_sk") as "base_customers",
      count(distinct "cooperative"."_virt_filter_customer_sk_2834222161828100") as "with_web",
      count(distinct "cooperative"."_virt_filter_customer_sk_7731278412084335") as "with_cat",
      count(distinct "cooperative"."_virt_filter_customer_sk_569015855313349") as "with_either"
  FROM
      "cooperative"
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_filtered_by_1045319072465155_grouped_by_ss.date_dim.moy_ss.date_dim.year_at_ss_date_dim_moy_ss_date_dim_year_at_local_ss_moy_local_ss_year onto ss.item.item_at_ss_item_item_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number_at_ss_item_item_sk_ss_ticket_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe2.preql --run-and-delete`

  ```text
  Resolution error in probe2.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: cs.sold_date.date_dim_at_cs_sold_date_date_sk_filtered_by_1481244836956823_grouped_by_cs.sold_date.moy_cs.sold_date.year_at_cs_sold_date_moy_cs_sold_date_year_at_local_cs_moy_local_cs_year onto cs.catalog_sales_at_cs_item_item_sk_cs_order_number_join_cs.item.item_at_cs_item_item_sk_at_cs_item_item_sk_cs_order_number_at_cs_item_item_sk_cs_order_number. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.date_dim.date_dim_at_ss_date_dim_date_sk_filtered_by_1045319072465155_grouped_by_ss.date_dim.moy_ss.date_dim.year_at_ss_date_dim_moy_ss_date_dim_year_at_local_ss_moy_local_ss_year onto ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.item.item_sk_ss.list_price_ss.quantity_at_ss_item_item_sk_ss_list_price_ss_quantity_at_local_prod_ss_item_item_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  Syntax error in probe5.preql: union arm 0 projects 3 column(s) but the output signature declares 1. Each arm must project exactly one column per output item, in order.
  ```
- `trilogy file write probe_join.preql --run-and-delete`

  ```text
  Syntax error in probe_join.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 5, column 1)
  ```
- `trilogy file write probe_music.preql --run-and-delete`

  ```text
  Unexpected error in probe_music.preql: Could not render the query: Missing source reference to i.category; Missing source reference to i.item_id; Missing source reference to ss.item.item_id. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  quizzical as (
  SELECT
      CASE WHEN INVALID_REFERENCE_BUG<Missing source reference to i.category> = 'Music' THEN INVALID_REFERENCE_BUG<Missing source reference to i.item_id> ELSE NULL END as "music_item_codes"
  ),
  highfalutin as (
  SELECT
      CASE WHEN exists (select 1 from quizzical where quizzical."music_item_codes" is not distinct from INVALID_REFERENCE_BUG<Missing source reference to ss.item.item_id>) THEN INVALID_REFERENCE_BUG<Missing source reference to ss.item.item_id> ELSE NULL END as "_virt_filter_item_id_8670440345985343",
      INVALID_REFERENCE_BUG<Missing source reference to ss.item.item_id> as "ss_item_item_id"
  FROM
      "quizzical")
  SELECT
      count("highfalutin"."_virt_filter_item_id_8670440345985343") as "semijoin_distinct_codes",
      count("highfalutin"."ss_item_item_id") as "total_codes"
  FROM
      "highfalutin"
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.item.item_at_ss_item_item_sk_at_ss_item_item_sk onto ss.customer.customer_address.customer_address_at_ss_customer_customer_address_address_sk_grouped_by_ss.customer.customer_address.gmt_offset_at_ss_customer_customer_address_gmt_offset_at_local_ca_gmt. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe5.preql --run-and-delete`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file write probe_fields.preql --run-and-delete`

  ```text
  Syntax error in probe_fields.preql: Output column 'store_sk' renames 'local.store_sk' back to the name of an existing concept 'store_sk' that 'local.store_sk' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'store_sk_out').
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Output column 'rnk' renames 'local.rnk' back to the name of an existing concept 'rnk' (defined at line 4) that 'local.rnk' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'rnk_out').
  ```
- `trilogy file write probe_rnk.preql --run-and-delete`

  ```text
  Syntax error in probe_rnk.preql: Output column 'total' renames 'local.total' back to the name of an existing concept 'total' (defined at line 3) that 'local.total' is derived from, so the rename refers back to itself. Use a distinct output name (e.g. 'total_out').
  ```
- `trilogy file write probe3.preql --run-and-delete`

  ```text
  Resolution error in probe3.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.ticket_number_at_ss_ticket_number onto ss.date_dim.date_dim_at_ss_date_dim_date_sk_join_ss.store_sales_at_ss_item_item_sk_ss_ticket_number_grouped_by_ss.date_dim.date_sk_ss.ticket_number_at_ss_date_dim_date_sk_ss_ticket_number_at_ss_date_dim_date_sk_ss_ticket_number_at_ss_date_dim_date_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write answer_1772060640.preql --run`

  ```text
  Syntax error in answer_1772060640.preql: Impossible comparison in SubselectComparison(left=ref:ss.store.county, right=('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County'), operator=<ComparisonOperator.IN: 'in'>): 'Orange County' can never match a declared value of enum<'Williamson County'> — fix the constant, or update the enum declaration if the domain is stale
  ```
- `trilogy file write probe_chan.preql --run-and-delete`

  ```text
  Syntax error in probe_chan.preql: union(...) requires at least two relational arms.
  ```
- `trilogy file write answer_2869182220.preql --run`

  ```text
  Resolution error in answer_2869182220.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_local__ret_item_sk onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_local__ret_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_ret.preql --run-and-delete`

  ```text
  Resolution error in probe_ret.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_local__ret_item_sk onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_local__ret_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_join.preql --run-and-delete`

  ```text
  Resolution error in probe_join.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.store_returns_at_sr_item_item_sk_sr_ticket_number onto c.customer_address.customer_address_at_c_customer_address_address_sk_join_c.customer_at_c_customer_sk_join_c.customer_demographics.customer_demographics_at_c_customer_demographics_demo_sk_join_c.household_demographics.household_demographics_at_c_household_demographics_demo_sk_join_c.household_demographics.income_band.income_band_at_c_household_demographics_income_band_income_band_sk_at_c_customer_sk_filtered_by_1921616464955258_at_c_customer_sk_grouped_by_local._cust_customer_id_local._cust_demo_sk_local._cust_first_name_local._cust_last_name_at_local__cust_customer_id_local__cust_demo_sk_local__cust_first_name_local__cust_last_name_at_cust_customer_id_cust_demo_sk_cust_first_name_cust_last_name_at_cust_customer_id_cust_demo_sk_cust_first_name_cust_last_name_local_full_name. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  Resolution error in probe_a.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.item.item_at_sr_item_item_sk_at_local__ret_item_sk onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_local__ret_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_b.preql --run-and-delete`

  ```text
  Resolution error in probe_b.preql: Planner emitted a keyless join between row-bearing sources that share a join axis: sr.store_returns_at_sr_item_item_sk_sr_ticket_number_grouped_by_sr.ticket_number_at_sr_ticket_number_at_local__ret_ticket_number onto sr.customer_demographics.customer_demographics_at_sr_customer_demographics_demo_sk_at_local__ret_demo_sk. This would render as a cross join (ON 1=1) and fan out; the join axis was lost upstream. This is a planner bug.
  ```
- `trilogy file write probe_final.preql --run-and-delete`

  ```text
  {
    "event": "write",
    "path": "probe_final.preql",
    "bytes": 2585
  }
  {
    "event": "output_truncated",
    "dropped_events": 0,
    "note": "Output exceeded the tool cap; intermediate events dropped (any error/summary events are preserved below). Narrow the call (--regex, --show, fewer rows) to see the rest."
  }
  {
    "event": "error",
    "message": "Syntax error in probe_final.preql: Missing source map entry for catalog_sales.bill_customer.first_name with pseudonyms set(), have map: {'web_sales.bill_customer.customer_sk': {BuildDatasource(name='customer', columns=[BuildColumnAssignment(alias='c_cus
  …
  , _str='Grain<web_sales.bill_customer.customer_sk>', _str_no_condition='Grain<web_sales.bill_customer.customer_sk>', abstract=False), namespace='web_sales.bill_customer', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'web_sales.bill_customer.first_sales_date.date_sk', 'web_sales.bill_customer.customer_address.address_sk', 'web_sales.bill_customer.first_shipto_date.date_sk', 'web_sales.bill_customer.last_review_date.date_sk', 'web_sales.bill_customer.customer_demographics.demo_sk'})}}"
  }
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Syntax error in probe_verify.preql: SELECT output 'local.class_rev' is defined by an expression that references 'local.class_rev' itself (line 6). This is a recursive self-reference: an alias cannot redefine a name its own calculation reads. Rename the output to a distinct name (e.g. `... as class_rev_out`).
  ```

### `undefined-concept`

- `trilogy file write probe8.preql --run-and-delete`

  ```text
  Syntax error in probe8.preql: Undefined concept: yr (line 10, col 10, in ORDER BY). Suggestions: ['yr_cnt.yr']
  ```
- `trilogy file write answer_2940558602.preql --run`

  ```text
  Syntax error in answer_2940558602.preql: 2 undefined concept references; fix all before re-running:
    - all_chan.return_rank (line 60, col 44, in ORDER BY); did you mean: all_chan.returned_amount, all_chan.returned_units, all_chan.revenue, return_rank?
    - all_chan.currency_rank (line 60, col 82, in ORDER BY); did you mean: all_chan.revenue, all_chan.item_sk, all_chan.channel, currency_rank?
  ```
- `trilogy file write probe_rt.preql --run-and-delete`

  ```text
  Syntax error in probe_rt.preql: Undefined concept: ss.sold_date.year. Suggestions: ['ss.date_dim.year', 'ss.store.date_dim.year', 'ss.promotion.end_date.year', 'ws.sold_date.year', 'ws.ship_date.year', 'ws.promotion.end_date.year']
  ```
- `trilogy file write probe_dates.preql --run-and-delete`

  ```text
  Syntax error in probe_dates.preql: Undefined concept: d.date_dim.date (line 2, col 7, in WHERE). Suggestions: ['d.date_dim.day_name', 'd.date_dim.year', 'd.date_id', 'd.date']
  ```
- `trilogy file write probe_weeks.preql --run-and-delete`

  ```text
  Syntax error in probe_weeks.preql: Undefined concept: d.date_dim.year (line 2, col 7, in WHERE). Suggestions: ['d.date_dim.week_seq', 'd.date_id', 'd.year']
  ```
- `trilogy file write probe_dates.preql --run-and-delete`

  ```text
  Syntax error in probe_dates.preql: Undefined concept: d.date_dim.week_seq. Suggestions: ['d.week_seq', 'd.fy_week_seq']
  ```
- `trilogy file write probe_weeks.preql --run-and-delete`

  ```text
  Syntax error in probe_weeks.preql: Undefined concept: d.date_dim.week_seq. Suggestions: ['d.week_seq', 'd.fy_week_seq']
  ```
- `trilogy file write probe_data.preql --run-and-delete`

  ```text
  Syntax error in probe_data.preql: 3 undefined concept references; fix all before re-running:
    - o.store_sales.ticket_number (line 8, in SELECT); did you mean: o.store.street_number, o.store_sales.date_dim.year, o.store_sales.store.store_name, o.ticket_number?
    - o.store_sales.sales_price (line 9, in SELECT); did you mean: o.store_sales.date_dim.year, o.ext_sales_price, o.sales_price?
    - o.store_sales.date_dim.year (line 2, col 7, in WHERE); did you mean: o.store.date_dim.year, o.store.date_dim.fy_year, o.store.date_dim.qoy, o.date_dim.year, o.customer.first_sales_date.year, o.promotion.end_date.year?
  ```
- `trilogy file write probe_fields.preql --run-and-delete`

  ```text
  Syntax error in probe_fields.preql: Undefined concept: ss.store_sk (line 18, col 35, in WHERE). Suggestions: ['ss.store.store_sk', 'ss.customer_sk', 'ss.date_sk', 'store_sk']
  ```
- `trilogy file write probe_fields.preql --run-and-delete`

  ```text
  Syntax error in probe_fields.preql: Undefined concept: local.store_sk (line 18, col 35, in WHERE). Suggestions: ['ss.store.store_sk', 'store_sk_out', 'customer_sk', 'store_code']
  ```
- `trilogy file write answer_3036656719.preql --run`

  ```text
  Syntax error in answer_3036656719.preql: Undefined concept: _virt_agg_grouping_591157893606151.
  ```
- `trilogy file write probe_rnk.preql --run-and-delete`

  ```text
  Syntax error in probe_rnk.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe_rnk.preql --run-and-delete`

  ```text
  Syntax error in probe_rnk.preql: Undefined concept: _virt_agg_grouping_591157893606151.
  ```
- `trilogy file write probe_rnk.preql --run-and-delete`

  ```text
  Syntax error in probe_rnk.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe_rnk.preql --run-and-delete`

  ```text
  Syntax error in probe_rnk.preql: Undefined concept: _virt_agg_grouping_2599847655796781.
  ```
- `trilogy file write probe_rnk.preql --run-and-delete`

  ```text
  Syntax error in probe_rnk.preql: Undefined concept: _virt_agg_grouping_3513081987384927.
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  Syntax error in probe_store.preql: Undefined concept: local.store_sk (line 9, col 10, in ORDER BY). Suggestions: ['ss.store.store_sk', 'ss_store_sk', 'dim_store_sk', 'store_id']
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  Syntax error in probe_store.preql: Undefined concept: store_sk. Suggestions: ['ss.store.store_sk', 'ss_store_sk', 'dim_store_sk', 'store_id']
  ```
- `trilogy file write answer_755724379.preql --run`

  ```text
  Syntax error in answer_755724379.preql: Undefined concept: ws.date_dim.year. Suggestions: ['ws.sold_date.year', 'ws.ship_date.year', 'ws.time_dim.time', 'ss.date_dim.year', 'ss.store.date_dim.year', 'cs.call_center.date_dim.year']
  ```
- `trilogy file write probe_topnull.preql --run-and-delete`

  ```text
  Syntax error in probe_topnull.preql: 2 undefined concept references; fix all before re-running:
    - top_all.ss_store_store_name (line 37, in SELECT); did you mean: top_all.ss.store.store_name, top_all.ss.store.company_name, top_all.total, ss.store.store_name?
    - top_all.ss_store_store_name (line 38, in SELECT); did you mean: top_all.ss.store.store_name, top_all.ss.store.company_name, top_all.total, ss.store.store_name?
  ```
- `trilogy file write probe_join.preql --run-and-delete`

  ```text
  Syntax error in probe_join.preql: Undefined concept: ss.item_sk. Suggestions: ['ss.item.item_sk', 'ss.promotion.item.item_sk', 'ss.item.size', 'ss.item.units', 'sr28.item_sk', 'sr.item.item_sk']
  ```

### `join-resolution`

- `trilogy file write probe7.preql --run-and-delete`

  ```text
  Resolution error in probe7.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs_cnt}; {ws_cnt, yr, ws.sold_date.year, ws.sold_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe1.preql --run-and-delete`

  ```text
  Resolution error in probe1.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {bid, catid, cid, iid, prod, ss_moy, ss_year, ticket, ss.date_dim.year}; {cord, cs_moy, cs_year}; {word, ws_moy, ws_year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_verify.preql --run-and-delete`

  ```text
  Resolution error in probe_verify.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {cs_cust, cs_order, cs_profit, cs_sale_date}; {item_code, item_sk, ss_cust, ss_profit, ss_sold_date, store_code, store_name, ticket, ss.item.item_sk, ss.ticket_number}; {sr_cust, sr_loss, sr_return_date}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write answer_3770074305.preql --run`

  ```text
  Resolution error in answer_3770074305.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {i.current_price, i.item_sk, i.manufact_id, description, item_code}; {inv.date_dim.date, inv.date_dim.date, inv.quantity_on_hand, inv.quantity_on_hand}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_metrics.preql --run-and-delete`

  ```text
  Resolution error in probe_metrics.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 11). The requested concepts split into 2 disconnected subgraphs: {cs.item.item_sk, list_amt}; {refund_amt}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  Resolution error in probe_store.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {amt, q, yr, ss.date_dim.year, ss.item.category}; {ra, rq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_catalog.preql --run-and-delete`

  ```text
  Resolution error in probe_catalog.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cs.item.category, cs.sold_date.year, amt, q, yr}; {ra, rq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_web.preql --run-and-delete`

  ```text
  Resolution error in probe_web.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {amt, q, yr, ws.item.category, ws.sold_date.year}; {ra, rq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file write probe_validate.preql --run-and-delete`

  ```text
  Resolution error in probe_validate.preql: Discovery error: cannot merge all concepts into one connected query. The requested concepts split into 2 disconnected subgraphs: {_catalog_only_c}; {_catalog_only_i}. Are you missing a join or merge statement to relate them?
  ```

### `syntax-parse`

- `trilogy file write probe_751385098.preql --run-and-delete`

  ```text
  refused to write 'probe_751385098.preql': not syntactically valid Trilogy.

  Parse error:
   --> 6:5
    |
  6 |     (where ss.customer.customer_sk is not null and ss.date_dim.year = 2000
    |     ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...ws;

   with store_set as
       ??? (where ss.customer.customer_sk...
  ```
- `trilogy file write probe_web.preql --run-and-delete`

  ```text
  refused to write 'probe_web.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...wr.return_amt) as total_ret,
   ??? subset join wr.item.item_sk =
  ```
- `trilogy file write probe_music.preql --run-and-delete`

  ```text
  refused to write 'probe_music.preql': not syntactically valid Trilogy.

  Parse error:
    --> 10:5
     |
  10 | by *;
     |     ^---
     |
     = expected access_chain
  Location:
  ...) as not_in_music_codes,
   by * ??? ;
  ```
- `trilogy file write probe_metrics.preql --run-and-delete`

  ```text
  refused to write 'probe_metrics.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:18
    |
  4 | with cs_pairs as (
    |                  ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...ns as cr;

   with cs_pairs as ??? (
     select cs.item.item_sk as...
  ```
- `trilogy file write answer_2874833976.preql --run`

  ```text
  refused to write 'answer_2874833976.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:1
     |
  30 | where year(ss.date_dim.date) = 2000
     | ^---
     |
     = expected limit, order_by, or having
  Location:
  ...tore.state, ss.store.county)
   ??? where year(ss.date_dim.date) =...
  ```
- `trilogy file write probe_store.preql --run-and-delete`

  ```text
  refused to write 'probe_store.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | subset join sr.item.item_sk = ss.item.item_sk
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ate_dim.year in (2001, 2002)
   ??? subset join sr.item.item_sk =
  ```
- `trilogy file write probe_catalog.preql --run-and-delete`

  ```text
  refused to write 'probe_catalog.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | subset join cr.item.item_sk = cs.item.item_sk
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ld_date.year in (2001, 2002)
   ??? subset join cr.item.item_sk =
  ```
- `trilogy file write probe_web.preql --run-and-delete`

  ```text
  refused to write 'probe_web.preql': not syntactically valid Trilogy.

  Parse error:
   --> 5:1
    |
  5 | subset join wr.item.item_sk = ws.item.item_sk
    | ^---
    |
    = expected limit, order_by, THEN_LA, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...ld_date.year in (2001, 2002)
   ??? subset join wr.item.item_sk =
  ```
- `trilogy file write probe_comb.preql --run-and-delete`

  ```text
  refused to write 'probe_comb.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ombined.sp), 0) as other_sp,
   ??? union join store_agg.year = co...
  ```

### `cli-misuse`

- `trilogy file cat root/catalog_sales.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy file show root/store_sales.preql`

  ```text
  No such command 'show'.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```
- `trilogy explore root/items.preql`

  ```text
  Invalid value for 'PATH': File 'root/items.preql' does not exist.
  ```
- `trilogy file cat root/store_sales.preql`

  ```text
  No such command 'cat'.
  ```
- `trilogy explore root/orders.preql`

  ```text
  Invalid value for 'PATH': File 'root/orders.preql' does not exist.
  ```

### `no-output`

- `trilogy file write probe_selectivity.preql --run-and-delete`

  ```text
  Nothing was executed: parsed 4 definition statement(s) (2 imports, 1 concept, 1 rowset) but none produce output. Did you mean to include a SELECT statement, or run a refresh on datasources instead?
  ```
