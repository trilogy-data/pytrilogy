# V4 suite sweep — triage

Run: `TRILOGY_V4_DISCOVERY=1` over the full suite (per-file, 120s timeout),
clickhouse-server file excluded (82 env-only failures, need a live server).

Buckets: **correctness** = v4 crashes, hangs, or returns wrong rows/values
(→ add a comparison case to the eval harness). **structure** = query still
runs & (apparently) returns the same data, only the SQL shape/source differs
(→ logged here, no eval case; inventory later).

## Correctness (v4 crash / wrong result / hang)

### Root cause A — condition can't be placed past a row-shape barrier
`group_graph.py:497/436`: *"conditions cannot be pushed past row-shape
changes"* / *"row inputs … not reachable from any group"*. v4 refuses to
apply a filter whose inputs only exist after a window / unnest / aggregate /
rowset barrier; v3 applies it as a post-barrier filter. **One bug, 7 sites:**
- `complex/test_window_function_parsing.py::test_select` (user_rank < 10 past window)
- `engine/test_duckdb.py::test_not_value` (z = 1 past unnest)
- `modeling/hackernews/test_hackernews_queries.py::test_adhoc03` (type = story not reachable)
- `modeling/test_complex.py::test_window_alt` (filtered = 1 past unnest)
- `modeling/tpc_ds/test_queries.py::test_one` (total_returns > avg past rowset)
- `modeling/tpc_h/test_tpch_queries.py::test_two` (part.name like forest% not reachable), `::test_eleven`? see B
- `optimization/test_join_hoist.py::test_hoist_preserves_concepts_referenced_via_output_lineage` (web_cume > store_cume past aggregate)
- `modeling/tpc_h/test_tpch_queries.py::test_nineteen`-style (local.size = 15 not reachable)

### Root cause B — "Invalid input concepts to node" (missing parent)
`base_node.py:221`: a demanded concept has no non-hidden parent node.
- `complex/test_rowset.py::test_rowset_alias_collision_distinct_aggregates` (local.id)
- `modeling/tpc_h/test_tpch_queries.py::test_ten` (order.customer.nation.id)
- `scripts/test_refresh.py::test_refresh_multiple_aggregate_persists_with_shared_count` (__preql_internal.all_rows)

### Root cause C — "Invalid reference string found in query" (renderer)
`dialect/base.py:2306`: v4 emits a SQL ref the renderer can't bind.
- `engine/demo/test_demo_duckdb_subselect.py::test_subselect_closest_warehouse`
- `engine/test_duckdb_filter.py::test_array_inclusion_aggregate_one`, `::test_array_inclusion_aggregate`, `::test_in_subselect_with_inlined_datasource`
- `modeling/hackernews/test_hackernews_queries.py::test_adhoc02`
- `modeling/ncaa/test_ncaa.py::test_adhoc02`
- `modeling/usa_names/test_names.py::test_filter_constant`

### Root cause D — invalid SQL generated (duckdb BinderException)
- `modeling/aggregates/test_group_by.py::test_group_by` (GROUP BY clause cannot contain aggregates)
- `engine/test_duckdb.py::test_union` (Ambiguous reference to table "highfalutin")
- `modeling/tpc_h/test_tpch_queries.py::test_eleven` (germany_total_value must appear in GROUP BY)

### Root cause E — crash / unresolvable
- `engine/test_duckdb.py::test_global_aggregate_inclusion` (IndexError query_processor.py:344)
- `modeling/google_analytics/test_sane_ga_rendering.py::test_daily_job` (NoDatasourceException all_sites.device.id)
- `test_user_functions.py::test_user_function_import` (Could not locate column 'quad_test' — column dropped)

### Root cause F — wrong rows / values (executes, bad data)
- `engine/demo/test_demo_duckdb.py::test_demo_e2e` (Should return two rows), `::test_group_case_output`
- `engine/demo/test_demo_duckdb_import.py::test_demo_merge_rowset_e2e` (17 == 8)
- `engine/demo/test_demo_duckdb_subselect.py::test_subselect_correlated` (5 == 2), `::test_subselect_imported_namespace` (5 == 2)
- `engine/test_dataframe.py::test_dataframe_executor` (None == 67)
- `engine/test_duckdb.py::test_window_over_rollup_preserves_grouping_rows`, `::test_window_partition_by_grouping_level_over_rollup` (row mismatches), `::test_rowset` (9==3), `::test_rowset_join` (4==1), `::test_proper_basic_unnest_handling` (10==5)
- `engine/test_duckdb_filter.py::test_array_inclusion` (unnest_node assert), `::test_demo_filter_select` ((1,1)==(2,8)), `::test_filter_scalar_aggregate_not_restricted_by_staging`
- `modeling/aggregates/test_cardinality.py::test_key_fetch_cardinality` (3==2)
- `modeling/join_resolution/test_join_resolution.py::test_ambiguous_error_with_forced_join` (4==3), `::test_ambiguous_error_with_forced_join_order` (6==5) — v4 may not raise the expected ambiguity; verify
- `modeling/rides_example/test_ride_example.py::test_example_model` (1==4)
- `modeling/stocks/test_stocks.py::test_calculated_field` (value 5)
- `modeling/test_complex.py::test_rowset_with_addition` ((2,2,1)==(1,None,None))
- `modeling/test_core_concepts.py::test_filter_grain` (4==2), `::test_filtered_project` (2==1)
- `modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_having_nested`, `::test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate` (0==2), `::test_two_merge_aggregate_compacts_inline_window_query`, `::test_rowset_arithmetic_argument_keeps_precedence`
- `modeling/tpc_h/instantiated/tpc_h/test_instantiated_tpc_h.py::test_adhoc07`
- `modeling/tpc_h/test_tpch_queries.py::test_adhoc03` (150000==333), `::test_adhoc06`, `::test_eighteen` (5 vs 100 rows), `::test_twenty` (no results), `::test_twenty_one`, `::test_twenty_two` (no results)
- `optimization/test_constant_optimization.py::test_constant_filter` (extra rows)
- `test_query_processing.py::test_query_aggregation` (1==2)
- `complex/test_rowset.py::test_rowset_alias_name_collision`
- `complex/test_bound_conversion_existence.py::test_bound_conversion_existence_presto`

### Root cause G — v4 hangs / spins (timeout)
- `etl/test_duckdb.py` (whole file timed out — isolate query)
- `modeling/gcat/test_gcat.py` (timed out)
- `modeling/tpc_ds_duckdb/test_queries.py` (expected — test_set excludes spinning ids)
- `engine/demo/test_demo_duckdb_multi_table.py::test_rowset_shape` (timed out)

## Structure only (SQL/source differs, runs OK) — log, no eval case

- `discovery/test_aggregate_handling.py` (7) — asserts pre-aggregated source picked
- `discovery/test_aggregate_resolution_coverage.py` (15) — source/grain selection asserts
- `discovery/test_aggregates_comprehensive.py` (13) — "Expected X table" source asserts
- `discovery/test_discovery.py` (2) — exact-SQL history e2e equality
- `discovery/test_primary_source_aggregate_fallback.py` (3) — aggregate-fallback source asserts
- `discovery/test_year_alias_resolution.py` (1) — source asserts
- `engine/test_bigquery.py::test_date_diff_rendering`, `engine/test_sqlite.py::test_date_diff_rendering`, `test_show.py::test_show_bigquery` — dialect SQL rendering
- `engine/test_duckdb.py::test_anon_function_resolves_from_precomputed_source` (SQL assert) — verify via parity
- `engine/test_duckdb_filter.py::test_aggregate_filter_uses_having` (HAVING-in-SQL assert)
- `modeling/ncaa/test_ncaa.py::test_adhoc07`, `::test_adhoc08` (SQL shape asserts)
- `modeling/usa_names/test_names.py` regex-on-SQL: `test_aggregate_filter_anonymous`, `test_aggregate_filter`, `test_aggregate_filter_short_syntax`, `test_group_by_with_existing`, `test_filter_constant_with_constant`
- `optimization/test_inlining.py::test_select_literal_is_rendered_with_aggregate_projection` (FULL JOIN on 1=1 — verify not a fan-out correctness issue)
- `optimization/test_merge_basic.py::test_inline_filter_basic` ("Expected no subquery")
- `optimization/test_constant_optimization.py::test_constant_optimization`, `::test_compile_statement_with_params_returns_sql_and_dict` (param/render shape)
- `persistence/test_basic_persistence.py` (3) — "CASE not in" persisted DDL shape

**Note:** several "structure" items (FULL JOIN on 1=1, source selection) can
mask fan-out correctness bugs. The parity harness executes v3 vs v4 and
compares rows, so it will auto-promote any mis-labeled structure case to
correctness.
