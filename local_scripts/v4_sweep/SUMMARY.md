# V4 suite sweep

- fail: 43
- timeout: 3
- error: 0
- pass: 209
- no_tests: 3
- total failing node ids: 204

## timeout (3 files)

### etl/test_duckdb.py  (0 failures)

### modeling/gcat/test_gcat.py  (0 failures)

### modeling/tpc_ds_duckdb/test_queries.py  (0 failures)

## fail (43 files)

### complex/test_bound_conversion_existence.py  (1 failures)
- `tests/complex/test_bound_conversion_existence.py::test_bound_conversion_existence_presto`

### complex/test_rowset.py  (2 failures)
- `tests/complex/test_rowset.py::test_rowset_alias_name_collision`
- `tests/complex/test_rowset.py::test_rowset_alias_collision_distinct_aggregates`

### complex/test_window_function_parsing.py  (1 failures)
- `tests/complex/test_window_function_parsing.py::test_select`

### discovery/test_aggregate_handling.py  (7 failures)
- `tests/discovery/test_aggregate_handling.py::test_aggregate_handling`
- `tests/discovery/test_aggregate_handling.py::test_aggregate_handling_alias`
- `tests/discovery/test_aggregate_handling.py::test_aggregate_handling_abstract`
- `tests/discovery/test_aggregate_handling.py::test_partial_additive_aggregate_rollup_sql`
- `tests/discovery/test_aggregate_handling.py::test_partial_sum_aggregate_rollup_sql`
- `tests/discovery/test_aggregate_handling.py::test_partial_aggregate_rollup_rejects_unsupported_aggregates`
- `tests/discovery/test_aggregate_handling.py::test_combine_grand_total_with_joined_namespace_count`

### discovery/test_aggregate_resolution_coverage.py  (15 failures)
- `tests/discovery/test_aggregate_resolution_coverage.py::test_exact_grain_match_customer`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_exact_grain_match_date`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_exact_grain_match_customer_date`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_rollup_from_finer_to_grand_total`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_rollup_customer_date_to_customer`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_rollup_customer_date_to_date`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_filter_on_grain_in_select`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_filter_on_grain_not_in_select`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_partial_key_upgrade_via_dimension_table`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_partial_key_upgrade_with_filter`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_dimension_attribute_with_aggregate`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_dimension_filter_with_aggregate`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_multiple_aggregates_at_same_grain`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_rollup_with_multiple_grain_keys_dropped`
- `tests/discovery/test_aggregate_resolution_coverage.py::test_aggregate_with_order_by_and_limit`

### discovery/test_aggregates_comprehensive.py  (13 failures)
- `tests/discovery/test_aggregates_comprehensive.py::test_total_summary`
- `tests/discovery/test_aggregates_comprehensive.py::test_customer_aggregates`
- `tests/discovery/test_aggregates_comprehensive.py::test_product_aggregates`
- `tests/discovery/test_aggregates_comprehensive.py::test_daily_aggregates`
- `tests/discovery/test_aggregates_comprehensive.py::test_customer_product_aggregates`
- `tests/discovery/test_aggregates_comprehensive.py::test_customer_daily_aggregates`
- `tests/discovery/test_aggregates_comprehensive.py::test_product_daily_aggregates`
- `tests/discovery/test_aggregates_comprehensive.py::test_customer_product_daily_aggregates`
- `tests/discovery/test_aggregates_comprehensive.py::test_mouse_product_filter`
- `tests/discovery/test_aggregates_comprehensive.py::test_mouse_customer_filter`
- `tests/discovery/test_aggregates_comprehensive.py::test_high_value_customer_filter`
- `tests/discovery/test_aggregates_comprehensive.py::test_mixed_aggregation_and_dimension`
- `tests/discovery/test_aggregates_comprehensive.py::test_cross_dimensional_aggregation`

### discovery/test_discovery.py  (2 failures)
- `tests/discovery/test_discovery.py::test_history_e2e`
- `tests/discovery/test_discovery.py::test_history_e2e_non_materialized_field`

### discovery/test_primary_source_aggregate_fallback.py  (3 failures)
- `tests/discovery/test_primary_source_aggregate_fallback.py::test_partial_precomputed_uses_aggregate`
- `tests/discovery/test_primary_source_aggregate_fallback.py::test_partial_precomputed_uses_aggregate_with_filter_in_select`
- `tests/discovery/test_primary_source_aggregate_fallback.py::test_partial_precomputed_uses_aggregate_with_grain_filter`

### discovery/test_year_alias_resolution.py  (1 failures)
- `tests/discovery/test_year_alias_resolution.py::test_resolve_via_named_concept_uses_aggregate`

### engine/demo/test_demo_duckdb.py  (2 failures)
- `tests/engine/demo/test_demo_duckdb.py::test_demo_e2e`
- `tests/engine/demo/test_demo_duckdb.py::test_group_case_output`

### engine/demo/test_demo_duckdb_import.py  (1 failures)
- `tests/engine/demo/test_demo_duckdb_import.py::test_demo_merge_rowset_e2e`

### engine/demo/test_demo_duckdb_multi_table.py  (1 failures)
- `tests/engine/demo/test_demo_duckdb_multi_table.py::test_rowset_shape`

### engine/demo/test_demo_duckdb_subselect.py  (3 failures)
- `tests/engine/demo/test_demo_duckdb_subselect.py::test_subselect_correlated`
- `tests/engine/demo/test_demo_duckdb_subselect.py::test_subselect_closest_warehouse`
- `tests/engine/demo/test_demo_duckdb_subselect.py::test_subselect_imported_namespace`

### engine/test_bigquery.py  (1 failures)
- `tests/engine/test_bigquery.py::test_date_diff_rendering`

### engine/test_clickhouse_server.py  (82 failures)
- `tests/engine/test_clickhouse_server.py::test_raw_select_one`
- `tests/engine/test_clickhouse_server.py::test_today`
- `tests/engine/test_clickhouse_server.py::test_trilogy_const`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[add]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[subtract]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[multiply]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[divide]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[mod]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[power]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[abs]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[floor]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[ceil]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[round]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[sqrt]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[log10]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[log2]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[upper]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[lower]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[len]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[substring]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[strpos]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[contains]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[trim]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[ltrim]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[rtrim]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[replace]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[concat]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[split]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[hex]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[hash_md5]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[hash_sha256]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[regexp_contains]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[regexp_extract]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[regexp_replace]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[like]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[ilike]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[year]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[month]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[day]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[quarter]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[week]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[day_of_week]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[day_name]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[month_name]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[hour]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[minute]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[second]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[date_trunc]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[date_add]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[date_sub]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[date_diff]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[date_cast]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[datetime_cast]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[format_time]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[current_date_not_null]`
- `tests/engine/test_clickhouse_server.py::test_function_smoke[current_datetime_not_null]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[sum]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[avg]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[max]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[min]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[count]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[count_distinct]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[any]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[bool_or_true]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[bool_or_false]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[bool_and_true]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[bool_and_false]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[array_agg]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[array_sort]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[array_distinct]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[array_sum]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[array_to_string]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[array_transform]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[array_filter]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[generate_array]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[index_access]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[map_keys]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[map_values]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[map_access]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[struct_attr_int]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[struct_attr_float]`
- `tests/engine/test_clickhouse_server.py::test_aggregate_smoke[struct_attr_str]`

### engine/test_dataframe.py  (1 failures)
- `tests/engine/test_dataframe.py::test_dataframe_executor`

### engine/test_duckdb.py  (10 failures)
- `tests/engine/test_duckdb.py::test_window_over_rollup_preserves_grouping_rows`
- `tests/engine/test_duckdb.py::test_window_partition_by_grouping_level_over_rollup`
- `tests/engine/test_duckdb.py::test_rowset`
- `tests/engine/test_duckdb.py::test_rowset_join`
- `tests/engine/test_duckdb.py::test_union`
- `tests/engine/test_duckdb.py::test_global_aggregate_inclusion`
- `tests/engine/test_duckdb.py::test_regexp`
- `tests/engine/test_duckdb.py::test_not_value`
- `tests/engine/test_duckdb.py::test_proper_basic_unnest_handling`
- `tests/engine/test_duckdb.py::test_anon_function_resolves_from_precomputed_source`

### engine/test_duckdb_filter.py  (7 failures)
- `tests/engine/test_duckdb_filter.py::test_array_inclusion`
- `tests/engine/test_duckdb_filter.py::test_array_inclusion_aggregate_one`
- `tests/engine/test_duckdb_filter.py::test_array_inclusion_aggregate`
- `tests/engine/test_duckdb_filter.py::test_demo_filter_select`
- `tests/engine/test_duckdb_filter.py::test_aggregate_filter_uses_having`
- `tests/engine/test_duckdb_filter.py::test_in_subselect_with_inlined_datasource`
- `tests/engine/test_duckdb_filter.py::test_filter_scalar_aggregate_not_restricted_by_staging`

### engine/test_sqlite.py  (1 failures)
- `tests/engine/test_sqlite.py::test_date_diff_rendering`

### modeling/aggregates/test_cardinality.py  (1 failures)
- `tests/modeling/aggregates/test_cardinality.py::test_key_fetch_cardinality`

### modeling/aggregates/test_group_by.py  (1 failures)
- `tests/modeling/aggregates/test_group_by.py::test_group_by`

### modeling/google_analytics/test_sane_ga_rendering.py  (1 failures)
- `tests/modeling/google_analytics/test_sane_ga_rendering.py::test_daily_job`

### modeling/hackernews/test_hackernews_queries.py  (2 failures)
- `tests/modeling/hackernews/test_hackernews_queries.py::test_adhoc02`
- `tests/modeling/hackernews/test_hackernews_queries.py::test_adhoc03`

### modeling/join_resolution/test_join_resolution.py  (2 failures)
- `tests/modeling/join_resolution/test_join_resolution.py::test_ambiguous_error_with_forced_join`
- `tests/modeling/join_resolution/test_join_resolution.py::test_ambiguous_error_with_forced_join_order`

### modeling/ncaa/test_ncaa.py  (3 failures)
- `tests/modeling/ncaa/test_ncaa.py::test_adhoc02`
- `tests/modeling/ncaa/test_ncaa.py::test_adhoc07`
- `tests/modeling/ncaa/test_ncaa.py::test_adhoc08`

### modeling/rides_example/test_ride_example.py  (1 failures)
- `tests/modeling/rides_example/test_ride_example.py::test_example_model`

### modeling/stocks/test_stocks.py  (1 failures)
- `tests/modeling/stocks/test_stocks.py::test_calculated_field`

### modeling/test_complex.py  (2 failures)
- `tests/modeling/test_complex.py::test_rowset_with_addition`
- `tests/modeling/test_complex.py::test_window_alt`

### modeling/test_core_concepts.py  (2 failures)
- `tests/modeling/test_core_concepts.py::test_filter_grain`
- `tests/modeling/test_core_concepts.py::test_filtered_project`

### modeling/tpc_ds/test_queries.py  (1 failures)
- `tests/modeling/tpc_ds/test_queries.py::test_one`

### modeling/tpc_ds_duckdb/test_non_benchmark_queries.py  (4 failures)
- `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_having_nested`
- `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_def_wrapped_filtered_aggregate_in_basic_expression_keeps_aggregate`
- `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_two_merge_aggregate_compacts_inline_window_query`
- `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_rowset_arithmetic_argument_keeps_precedence`

### modeling/tpc_h/instantiated/tpc_h/test_instantiated_tpc_h.py  (1 failures)
- `tests/modeling/tpc_h/instantiated/tpc_h/test_instantiated_tpc_h.py::test_adhoc07`

### modeling/tpc_h/test_tpch_queries.py  (9 failures)
- `tests/modeling/tpc_h/test_tpch_queries.py::test_adhoc03`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_adhoc06`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_two`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_ten`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_eleven`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_eighteen`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_twenty`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_twenty_one`
- `tests/modeling/tpc_h/test_tpch_queries.py::test_twenty_two`

### modeling/usa_names/test_names.py  (6 failures)
- `tests/modeling/usa_names/test_names.py::test_aggregate_filter_anonymous`
- `tests/modeling/usa_names/test_names.py::test_aggregate_filter`
- `tests/modeling/usa_names/test_names.py::test_aggregate_filter_short_syntax`
- `tests/modeling/usa_names/test_names.py::test_group_by_with_existing`
- `tests/modeling/usa_names/test_names.py::test_filter_constant`
- `tests/modeling/usa_names/test_names.py::test_filter_constant_with_constant`

### optimization/test_constant_optimization.py  (3 failures)
- `tests/optimization/test_constant_optimization.py::test_constant_optimization`
- `tests/optimization/test_constant_optimization.py::test_constant_filter`
- `tests/optimization/test_constant_optimization.py::test_compile_statement_with_params_returns_sql_and_dict`

### optimization/test_inlining.py  (1 failures)
- `tests/optimization/test_inlining.py::test_select_literal_is_rendered_with_aggregate_projection`

### optimization/test_join_hoist.py  (1 failures)
- `tests/optimization/test_join_hoist.py::test_hoist_preserves_concepts_referenced_via_output_lineage`

### optimization/test_merge_basic.py  (1 failures)
- `tests/optimization/test_merge_basic.py::test_inline_filter_basic`

### persistence/test_basic_persistence.py  (3 failures)
- `tests/persistence/test_basic_persistence.py::test_derivations`
- `tests/persistence/test_basic_persistence.py::test_derivations_reparse`
- `tests/persistence/test_basic_persistence.py::test_persist_with_where`

### scripts/test_refresh.py  (1 failures)
- `tests/scripts/test_refresh.py::test_refresh_multiple_aggregate_persists_with_shared_count`

### test_query_processing.py  (1 failures)
- `tests/test_query_processing.py::test_query_aggregation`

### test_show.py  (1 failures)
- `tests/test_show.py::test_show_bigquery`

### test_user_functions.py  (1 failures)
- `tests/test_user_functions.py::test_user_function_import`
