# V4 failure reasons (clickhouse + timeouts excluded)

## complex

### complex/test_bound_conversion_existence.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\complex\test_bound_conversion_existence.py:103: AssertionError:
```

### complex/test_rowset.py  (2 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\complex\test_rowset.py:159: AssertionError: buyers_a.cust_id should project bill, sql was:
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py:221: ValueError: Invalid input concepts to node! ['local.id'] are missing non-hidden parent nodes; have {'local.grp_key'} and hidden set() from root {'local.grp_key'}
```

### complex/test_window_function_parsing.py  (1 nodes)
```
C:\\Users\\ethan\\coding_projects\\pytrilogy\\trilogy\\core\\processing\\v4_helper\\group_graph.py:497: ValueError: Atom local.user_rank < 10 would be injected at grp:window:d0:local.user_id, which is downstream of d0 barrier(s) ['grp:aggregate:d0:\u2205:dedup:local.post_id']; conditions cannot be pushed past row-shape changes.
```

## discovery

### discovery/test_aggregate_handling.py  (7 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_handling.py:71: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_handling.py:97: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_handling.py:157: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_handling.py:251: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_handling.py:268: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_handling.py:313: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_handling.py:413: AssertionError:
```

### discovery/test_aggregate_resolution_coverage.py  (15 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:179: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:186: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:195: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:206: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:216: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:223: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:236: AssertionError: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:256: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:276: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:293: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:314: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:328: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:346: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:369: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregate_resolution_coverage.py:380: AssertionError:
```

### discovery/test_aggregates_comprehensive.py  (13 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:27: AssertionError: Expected total_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:43: AssertionError: Expected customer_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:60: AssertionError: Expected product_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:78: AssertionError: Expected daily_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:95: AssertionError: Expected customer_product_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:112: AssertionError: Expected customer_daily_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:130: AssertionError: Expected product_daily_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:148: AssertionError: Expected customer_product_daily_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:166: AssertionError: Expected mouse_product_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:184: AssertionError: Expected mouse_customer_summary table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:201: AssertionError: Expected high_value_customers table, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:279: AssertionError: Expected customer tables, got:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_aggregates_comprehensive.py:346: AssertionError: Expected appropriate table, got: SELECT
```

### discovery/test_discovery.py  (2 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_discovery.py:85: assert 'SELECT\n    ...OUP BY\n    1' == 'SELECT\n    ...enue_for_two"'
c:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_discovery.py:112: assert 'WITH \nquizz...OUP BY\n    1' == 'SELECT\n    ...ue_for_sarah"'
```

### discovery/test_primary_source_aggregate_fallback.py  (3 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_primary_source_aggregate_fallback.py:153: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_primary_source_aggregate_fallback.py:166: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_primary_source_aggregate_fallback.py:186: AssertionError:
```

### discovery/test_year_alias_resolution.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\discovery\test_year_alias_resolution.py:87: AssertionError: Expected flight_count_by_year, got: SELECT
```

## engine

### engine/demo/test_demo_duckdb.py  (2 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\demo\test_demo_duckdb.py:608: AssertionError: Should return two rows
```

### engine/demo/test_demo_duckdb_import.py  (1 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\engine\demo\test_demo_duckdb_import.py:223: AssertionError: assert 17 == 8
```

### engine/demo/test_demo_duckdb_multi_table.py  (1 nodes)
```
<<TIMEOUT>>
```

### engine/demo/test_demo_duckdb_subselect.py  (3 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\demo\test_demo_duckdb_subselect.py:64: AssertionError: assert 5 == 2
C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py:2306: ValueError: Invalid reference string found in query:
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\demo\test_demo_duckdb_subselect.py:218: AssertionError: assert 5 == 2
```

### engine/test_bigquery.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_bigquery.py:46: AssertionError: assert 'DATE_ADD(current_date(), INTERVAL -30 day)' in 'SELECT\n    current_date() as `today`\n\n'
```

### engine/test_dataframe.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_dataframe.py:51: assert None == 67
```

### engine/test_duckdb.py  (10 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb.py:116: AssertionError: assert [('a', None, ..., 10, 3), ...] == [(None, None,...', 'x', 5, 1)]
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb.py:155: AssertionError: assert [('a', None, ...5, 0, 3), ...] == [(None, None,...'x', 5, 0, 3)]
C:\\Users\\ethan\\coding_projects\\pytrilogy\\trilogy\\core\\processing\\v4_helper\\group_graph.py:497: ValueError: Atom local.z = 1 would be injected at grp:window:d0:local.x, which is downstream of d0 barrier(s) ['grp:unnest:d0:\u2205']; conditions cannot be pushed past row-shape changes.
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb.py:516: assert 9 == 3
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\query_processor.py:344: IndexError: list index out of range
C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\duckdb_engine\__init__.py:150: sqlalchemy.exc.ProgrammingError: (_duckdb.BinderException) Binder Error: Ambiguous reference to table "highfalutin" (duplicate alias "highfalutin", explicitly alias one of the tables using "AS my_alias")
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb.py:1079: AssertionError: assert 4 == 1
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb.py:1202: AssertionError: unnest(ref:local._virt_3642345836003314)
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb.py:1350: assert 10 == 5
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb.py:1837: AssertionError: SELECT
```

### engine/test_duckdb_filter.py  (7 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\unnest_node.py:40: AssertionError
C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py:2306: ValueError: Invalid reference string found in query:
C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py:2306: ValueError: Invalid reference string found in query:
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb_filter.py:159: assert (1, 1) == (2, 8)
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb_filter.py:234: assert 'HAVING' in '\nWITH \ncheerful as (\nSELECT\n    "sales"."order_id" as "order_id",\n    count("sales"."warehouse_id") as "_virt_ag...order_id" ELSE NULL END as "multi_warehouse_orders"\nFROM\n    "cheerful"\nORDER BY \n    "multi_warehouse_orders" asc'
C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py:2306: ValueError: Invalid reference string found in query: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_duckdb_filter.py:591: AssertionError:
```

### engine/test_sqlite.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\engine\test_sqlite.py:48: assert 'datetime(' in 'SELECT\n    date(\'now\') as "today"\n'
```

## modeling

### modeling/aggregates/test_cardinality.py  (1 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\modeling\aggregates\test_cardinality.py:24: AssertionError: assert 3 == 2
```

### modeling/aggregates/test_group_by.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\duckdb_engine\__init__.py:150: sqlalchemy.exc.ProgrammingError: (_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!
```

### modeling/google_analytics/test_sane_ga_rendering.py  (1 nodes)
```
INFO     trilogy:query_processor.py:536 [QUERY BUILD] building query node for [ref:all_sites.event_date, ref:all_sites.device.category, ref:local.user_count_detail, ref:local.static] grain Grain<all_sites.device.category,all_sites.event_date>
INFO     trilogy:query_processor.py:568 [QUERY BUILD] getting source datasource for outputs [all_sites.event_date@Grain<all_sites._row_id>, all_sites.device.category@Grain<all_sites.device.id>, local.user_count_detail@Grain<all_sites.device.category,all_sites.event_date>, local.static@Grain<all_sites.device.category,all_sites.event_date>] grain Grain<all_sites.device.category,all_sites.event_date>
INFO     trilogy:select_node.py:99 	[GEN_SELECT_NODE] Skipping select node generation for [all_sites._row_id@Grain<all_sites._row_id>, all_sites.device.category@Grain<all_sites.device.id>, all_sites.device.id@Grain<all_sites.device.id>, all_sites.event_date@Grain<all_sites._row_id>, all_sites.user_pseudo_id@Grain<all_sites._row_id>] as it + optional includes non-materialized concepts (looking for all lcl['all_sites._row_id', 'all_sites.device.category', 'all_sites.device.id', 'all_sites.event_date', 'all_sites.user_pseudo_id'], missing {'all_sites.device.id'}).
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\node_generators\select_node.py:55: trilogy.core.exceptions.NoDatasourceException: No datasource exists for root concept all_sites.device.id@Grain<all_sites.device.id>, and no resolvable pseudonyms found from set(). This query is unresolvable from your environment. Check your datasources and imports to make sure this concept is bound.
=========================== short test summary info ===========================
FAILED tests/modeling/google_analytics/test_sane_ga_rendering.py::test_daily_job
1 failed in 4.21s
```

### modeling/hackernews/test_hackernews_queries.py  (2 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py:2306: ValueError: Invalid reference string found in query:
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py:436: ValueError: Could not place condition atom root_parent.type = story: row inputs ['root_parent.type'] not reachable from any group.
```

### modeling/join_resolution/test_join_resolution.py  (2 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\join_resolution\test_join_resolution.py:53: assert 4 == 3
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\join_resolution\test_join_resolution.py:92: assert 6 == 5
```

### modeling/ncaa/test_ncaa.py  (3 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py:2306: ValueError: Invalid reference string found in query: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\ncaa\test_ncaa.py:165: AssertionError: WITH
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\ncaa\test_ncaa.py:181: AssertionError:
```

### modeling/rides_example/test_ride_example.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\rides_example\test_ride_example.py:29: assert 1 == 4
```

### modeling/stocks/test_stocks.py  (1 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\modeling\stocks\test_stocks.py:342: AssertionError: 5
```

### modeling/test_complex.py  (2 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\modeling\test_complex.py:43: assert (2, 2, 1) == (1, None, None)
C:\\Users\\ethan\\coding_projects\\pytrilogy\\trilogy\\core\\processing\\v4_helper\\group_graph.py:497: ValueError: Atom local.filtered = 1 would be injected at grp:window:d0:local.nums, which is downstream of d0 barrier(s) ['grp:unnest:d0:\u2205']; conditions cannot be pushed past row-shape changes.
```

### modeling/test_core_concepts.py  (2 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\modeling\test_core_concepts.py:174: assert 4 == 2
c:\Users\ethan\coding_projects\pytrilogy\tests\modeling\test_core_concepts.py:233: AssertionError: assert 2 == 1
```

### modeling/tpc_ds/test_queries.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py:497: ValueError: Atom ca_2022.total_returns > (multiply(1.2,local.avg_store_returns@Grain<ca_2022.returns.store.id>)) would be injected at grp:aggregate:d0:ca_2022.returns.store.id, which is downstream of d0 barrier(s) ['grp:rowset:d0:ca_2022.returns.customer.id|ca_2022.returns.return_date.year|ca_2022.returns.store.id|ca_2022.returns.store.state:rowset:ca_2022']; conditions cannot be pushed past row-shape changes.
```

### modeling/tpc_ds_duckdb/test_non_benchmark_queries.py  (4 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_ds_duckdb\test_non_benchmark_queries.py:216: AssertionError: (2003, 2003, 534, 2003, 127)
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_ds_duckdb\test_non_benchmark_queries.py:339: assert 0 == 2
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_ds_duckdb\test_non_benchmark_queries.py:417: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_ds_duckdb\test_non_benchmark_queries.py:426: AssertionError:
```

### modeling/tpc_h/instantiated/tpc_h/test_instantiated_tpc_h.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_h\instantiated\tpc_h\test_instantiated_tpc_h.py:17: AssertionError:
```

### modeling/tpc_h/test_tpch_queries.py  (9 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_h\test_tpch_queries.py:156: assert 150000 == 333
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_h\test_tpch_queries.py:194: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py:436: ValueError: Could not place condition atom local.size = 15: row inputs ['local.size'] not reachable from any group.
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\nodes\base_node.py:221: ValueError: Invalid input concepts to node! ['order.customer.nation.id'] are missing non-hidden parent nodes; have {'order.customer.account_balance', 'local.extended_price', 'order.id', 'order.customer.address', 'order.customer.phone', 'local.line_no', 'order.customer.id', 'order.customer.name', 'local.discount', 'order.customer.nation.name', 'order.customer.comment'} and hidden set() from root {'order.customer.account_balance', 'local.extended_price', 'order.id', 'order.customer.address', 'order.customer.phone', 'local.line_no', 'order.customer.id', 'order.customer.name', 'local.discount', 'order.customer.nation.name', 'order.customer.comment'}
C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\duckdb_engine\__init__.py:150: sqlalchemy.exc.ProgrammingError: (_duckdb.BinderException) Binder Error: column germany_total_value must appear in the GROUP BY clause or be used in an aggregate function
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_h\test_tpch_queries.py:100: AssertionError: Row count mismatch: expected 5, got 100
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py:436: ValueError: Could not place condition atom part.name like forest%: row inputs ['part.name'] not reachable from any group.
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_h\test_tpch_queries.py:97: AssertionError: No results returned
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\tpc_h\test_tpch_queries.py:97: AssertionError: No results returned
```

### modeling/usa_names/test_names.py  (6 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\usa_names\test_names.py:103: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\usa_names\test_names.py:146: assert None
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\usa_names\test_names.py:181: assert None
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\usa_names\test_names.py:203: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py:2306: ValueError: Invalid reference string found in query: SELECT
C:\Users\ethan\coding_projects\pytrilogy\tests\modeling\usa_names\test_names.py:341: AssertionError:
```

## optimization

### optimization/test_constant_optimization.py  (3 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\optimization\test_constant_optimization.py:30: AssertionError:
C:\Users\ethan\coding_projects\pytrilogy\tests\optimization\test_constant_optimization.py:54: assert [(1,), (2,), ...,), (6,), ...] == [(1,)]
C:\Users\ethan\coding_projects\pytrilogy\tests\optimization\test_constant_optimization.py:88: assert ':filter_val' in 'SELECT\n    unnest(:_virt_705671875681119) as "array"\n'
```

### optimization/test_inlining.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\tests\optimization\test_inlining.py:159: assert 'FULL JOIN' not in '\nWITH \nqu...rful" on 1=1'
```

### optimization/test_join_hoist.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\group_graph.py:497: ValueError: Atom local.web_cume > local.store_cume would be injected at grp:window:d0:local.day, which is downstream of d0 barrier(s) ['grp:aggregate:d0:local.day']; conditions cannot be pushed past row-shape changes.
```

### optimization/test_merge_basic.py  (1 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\optimization\test_merge_basic.py:37: AssertionError: Expected no subquery, got:
```

## persistence

### persistence/test_basic_persistence.py  (3 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\persistence\test_basic_persistence.py:118: AssertionError: assert 'CASE' not in '\nWITH \nqu...quizzical`\n'
c:\Users\ethan\coding_projects\pytrilogy\tests\persistence\test_basic_persistence.py:192: AssertionError: assert 'CASE' not in '\nWITH \nqu...quizzical`\n'
c:\Users\ethan\coding_projects\pytrilogy\tests\persistence\test_basic_persistence.py:308: AssertionError: assert 'CASE' not in '\nWITH \nqu...quizzical`\n'
```

## scripts

### scripts/test_refresh.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\trilogy\execution\state\state_store.py:587: trilogy.execution.state.state_store.RefreshAssetError: Failed to refresh datasource 'flight_count_total' (stale because: forced rebuild): ValueError: Invalid input concepts to node! ['__preql_internal.all_rows'] are missing non-hidden parent nodes; have {'local.id', 'local.data_through'} and hidden set() from root {'local.id', 'local.data_through'}
```

## test_query_processing.py

### test_query_processing.py  (1 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\test_query_processing.py:174: assert 1 == 2
```

## test_show.py

### test_show.py  (1 nodes)
```
c:\Users\ethan\coding_projects\pytrilogy\tests\test_show.py:55: AssertionError:
```

## test_user_functions.py

### test_user_functions.py  (1 nodes)
```
C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\sqlalchemy\engine\result.py:213: AttributeError: Could not locate column in row for column 'quad_test'
```
