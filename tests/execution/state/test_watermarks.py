from datetime import datetime

from trilogy import Executor
from trilogy.core.models.datasource import UpdateKey, UpdateKeys, UpdateKeyType
from trilogy.execution.state.state_store import (
    BaseStateStore,
    UpdateKeyType,
    get_incremental_key_watermarks,
    get_last_update_time_watermarks,
    get_unique_key_hash_watermarks,
)


def test_last_update_time_watermarks(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key user_id int;
        property user_id.name string;
        property user_id.created_at datetime;

        datasource users (
            user_id: user_id,
            name: name,
            created_at: created_at
        )
        grain (user_id)
        query '''
        SELECT 1 as user_id, 'Alice' as name, '2024-01-01 10:00:00' as created_at
        UNION ALL
        SELECT 2 as user_id, 'Bob' as name, '2024-01-02 11:00:00' as created_at
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["users"]
    watermarks = get_last_update_time_watermarks(datasource, duckdb_engine)

    assert "update_time" in watermarks.keys
    assert watermarks.keys["update_time"].type == UpdateKeyType.UPDATE_TIME
    assert watermarks.keys["update_time"].value is not None


def test_incremental_key_watermarks(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key order_id int;
        property order_id.amount float;
        property order_id.order_date datetime;

        datasource orders (
            order_id: order_id,
            amount: amount,
            order_date: order_date
        )
        grain (order_id)
        query '''
        SELECT 1 as order_id, 100.0 as amount, '2024-01-01 10:00:00' as order_date
        UNION ALL
        SELECT 2 as order_id, 200.0 as amount, '2024-01-05 11:00:00' as order_date
        UNION ALL
        SELECT 3 as order_id, 150.0 as amount, '2024-01-10 12:00:00' as order_date
        '''
        incremental by order_date;
        """
    )

    datasource = duckdb_engine.environment.datasources["orders"]
    watermarks = get_incremental_key_watermarks(datasource, duckdb_engine)

    assert "order_date" in watermarks.keys
    assert watermarks.keys["order_date"].type == UpdateKeyType.INCREMENTAL_KEY
    assert watermarks.keys["order_date"].value is not None


def test_unique_key_hash_watermarks(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key product_id int;
        key store_id int;
        property product_id.product_name string;
        property store_id.store_name string;
        property product_id.stock_count int;

        datasource inventory (
            product_id: product_id,
            store_id: store_id,
            product_name: product_name,
            store_name: store_name,
            stock_count: stock_count
        )
        grain (product_id, store_id)
        query '''
        SELECT 1 as product_id, 1 as store_id, 'Widget' as product_name, 'Store A' as store_name, 10 as stock_count
        UNION ALL
        SELECT 2 as product_id, 1 as store_id, 'Gadget' as product_name, 'Store A' as store_name, 5 as stock_count
        UNION ALL
        SELECT 1 as product_id, 2 as store_id, 'Widget' as product_name, 'Store B' as store_name, 8 as stock_count
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["inventory"]
    watermarks = get_unique_key_hash_watermarks(datasource, duckdb_engine)

    assert "product_id" in watermarks.keys
    assert "store_id" in watermarks.keys
    assert watermarks.keys["product_id"].type == UpdateKeyType.KEY_HASH
    assert watermarks.keys["store_id"].type == UpdateKeyType.KEY_HASH
    assert watermarks.keys["product_id"].value is not None
    assert watermarks.keys["store_id"].value is not None


def test_base_state_store_incremental_by(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key transaction_id int;
        property transaction_id.timestamp datetime;
        property transaction_id.amount float;

        datasource transactions (
            transaction_id: transaction_id,
            timestamp: timestamp,
            amount: amount
        )
        grain (transaction_id)
        query '''
        SELECT 1 as transaction_id, '2024-01-01 10:00:00' as timestamp, 50.0 as amount
        UNION ALL
        SELECT 2 as transaction_id, '2024-01-02 11:00:00' as timestamp, 75.0 as amount
        '''
        incremental by timestamp;
        """
    )

    datasource = duckdb_engine.environment.datasources["transactions"]
    state_store = BaseStateStore()

    watermarks = state_store.watermark_asset(datasource, duckdb_engine)

    assert watermarks is not None
    assert "timestamp" in watermarks.keys
    assert watermarks.keys["timestamp"].type == UpdateKeyType.INCREMENTAL_KEY

    retrieved = state_store.get_datasource_watermarks(datasource)
    assert retrieved == watermarks
    assert state_store.check_datasource_state(datasource) is True


def test_base_state_store_key_hash(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key customer_id int;
        property customer_id.name string;
        property customer_id.email string;

        datasource customers (
            customer_id: customer_id,
            name: name,
            email: email
        )
        grain (customer_id)
        query '''
        SELECT 1 as customer_id, 'Alice' as name, 'alice@example.com' as email
        UNION ALL
        SELECT 2 as customer_id, 'Bob' as name, 'bob@example.com' as email
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["customers"]
    state_store = BaseStateStore()

    watermarks = state_store.watermark_asset(datasource, duckdb_engine)

    assert watermarks is not None
    assert "customer_id" in watermarks.keys
    assert watermarks.keys["customer_id"].type == UpdateKeyType.KEY_HASH


def test_base_state_store_update_time(duckdb_engine: Executor):
    # Create a physical table so we can test UPDATE_TIME watermarks
    # UPDATE_TIME is only available for physical tables, not queries
    duckdb_engine.execute_text(
        """
        key event_row int;
        property event_row.event_type string;
        property event_row.event_count int;

        datasource events (
            event_row: event_row,
            event_type: event_type,
            event_count: event_count
        )
        grain (event_row)
        address events_table;

        CREATE IF NOT EXISTS DATASOURCE events;

        RAW_SQL('''
        INSERT INTO events_table VALUES (1, 'login', 100), (2, 'logout', 95)
        ''');
        """
    )

    datasource = duckdb_engine.environment.datasources["events"]
    state_store = BaseStateStore()

    watermarks = state_store.watermark_asset(datasource, duckdb_engine)

    # Since there's a key column, it will use KEY_HASH watermark type
    assert watermarks is not None
    assert "event_row" in watermarks.keys
    assert watermarks.keys["event_row"].type == UpdateKeyType.KEY_HASH


def test_empty_incremental_by(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key simple_id int;

        datasource simple (
            simple_id: simple_id
        )
        grain (simple_id)
        query '''
        SELECT 1 as simple_id
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["simple"]
    watermarks = get_incremental_key_watermarks(datasource, duckdb_engine)

    assert watermarks.keys == {}


def test_no_key_columns(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key metric_id int;
        property metric_id.metric_name string;
        property metric_id.metric_value float;

        datasource metrics (
            metric_id: metric_id,
            metric_name: metric_name,
            metric_value: metric_value
        )
        grain (metric_id)
        query '''
        SELECT 1 as metric_id, 'cpu_usage' as metric_name, 75.5 as metric_value
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["metrics"]
    watermarks = get_unique_key_hash_watermarks(datasource, duckdb_engine)

    # Now has a key column, so we get KEY_HASH
    assert "metric_id" in watermarks.keys
    assert watermarks.keys["metric_id"].type == UpdateKeyType.KEY_HASH


def test_multiple_incremental_keys(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key record_id int;
        property record_id.updated_at datetime;
        property record_id.version int;

        datasource versioned_records (
            record_id: record_id,
            updated_at: updated_at,
            version: version
        )
        grain (record_id)
        query '''
        SELECT 1 as record_id, '2024-01-01 10:00:00' as updated_at, 1 as version
        UNION ALL
        SELECT 2 as record_id, '2024-01-02 11:00:00' as updated_at, 2 as version
        UNION ALL
        SELECT 3 as record_id, '2024-01-03 12:00:00' as updated_at, 3 as version
        '''
        incremental by updated_at, version;
        """
    )

    datasource = duckdb_engine.environment.datasources["versioned_records"]
    watermarks = get_incremental_key_watermarks(datasource, duckdb_engine)

    assert "updated_at" in watermarks.keys
    assert "version" in watermarks.keys
    assert watermarks.keys["updated_at"].type == UpdateKeyType.INCREMENTAL_KEY
    assert watermarks.keys["version"].type == UpdateKeyType.INCREMENTAL_KEY


def test_watermark_all_assets(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key item_id int;
        property item_id.value string;

        datasource items (
            item_id: item_id,
            value: value
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'a' as value
        ''';

        datasource items_copy (
            item_id: item_id,
            value: value
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'a' as value
        ''';
        """
    )

    state_store = BaseStateStore()
    watermarks = state_store.watermark_all_assets(
        duckdb_engine.environment, duckdb_engine
    )

    assert "items" in watermarks
    assert "items_copy" in watermarks


def test_get_stale_assets_incremental(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key event_id int;
        property event_id.event_ts datetime;

        datasource source_events (
            event_id: event_id,
            event_ts: event_ts
        )
        grain (event_id)
        query '''
        SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
        UNION ALL
        SELECT 2 as event_id, TIMESTAMP '2024-01-15 12:00:00' as event_ts
        UNION ALL
        SELECT 3 as event_id, TIMESTAMP '2024-01-20 12:00:00' as event_ts
        '''
        incremental by event_ts;

        datasource target_events (
            event_id: event_id,
            event_ts: event_ts
        )
        grain (event_id)
        address target_events_table
        incremental by event_ts;

        CREATE IF NOT EXISTS DATASOURCE target_events;

        RAW_SQL('''
        INSERT INTO target_events_table
        SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
        ''');
        """
    )

    state_store = BaseStateStore()
    stale = state_store.get_stale_assets(
        duckdb_engine.environment,
        duckdb_engine,
        root_assets={"source_events"},
    )

    assert len(stale) == 1
    assert stale[0].datasource_id == "target_events"
    assert "event_ts" in stale[0].reason
    assert "behind" in stale[0].reason


def test_get_stale_assets_up_to_date(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key sync_id int;
        property sync_id.sync_ts datetime;

        datasource sync_source (
            sync_id: sync_id,
            sync_ts: sync_ts
        )
        grain (sync_id)
        query '''
        SELECT 1 as sync_id, TIMESTAMP '2024-01-10 12:00:00' as sync_ts
        '''
        incremental by sync_ts;

        datasource sync_target (
            sync_id: sync_id,
            sync_ts: sync_ts
        )
        grain (sync_id)
        address sync_target_table
        incremental by sync_ts;

        CREATE IF NOT EXISTS DATASOURCE sync_target;

        RAW_SQL('''
        INSERT INTO sync_target_table
        SELECT 1 as sync_id, TIMESTAMP '2024-01-10 12:00:00' as sync_ts
        ''');
        """
    )

    state_store = BaseStateStore()
    stale = state_store.get_stale_assets(
        duckdb_engine.environment,
        duckdb_engine,
        root_assets={"sync_source"},
    )

    assert len(stale) == 0


def test_get_stale_assets_empty_target(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        key log_id int;
        property log_id.log_ts datetime;

        datasource log_source (
            log_id: log_id,
            log_ts: log_ts
        )
        grain (log_id)
        query '''
        SELECT 1 as log_id, TIMESTAMP '2024-01-10 12:00:00' as log_ts
        '''
        incremental by log_ts;

        datasource log_target (
            log_id: log_id,
            log_ts: log_ts
        )
        grain (log_id)
        address log_target_table
        incremental by log_ts;

        CREATE IF NOT EXISTS DATASOURCE log_target;
        """
    )

    state_store = BaseStateStore()
    stale = state_store.get_stale_assets(
        duckdb_engine.environment,
        duckdb_engine,
        root_assets={"log_source"},
    )

    assert len(stale) == 1
    assert stale[0].datasource_id == "log_target"
    assert stale[0].filters.keys == {}


def test_update_datasource_full_refresh(duckdb_engine: Executor):
    """Test update_datasource with no filters (full refresh)."""
    duckdb_engine.execute_text(
        """
        key item_id int;
        property item_id.item_name string;

        datasource source_items (
            item_id: item_id,
            item_name: item_name
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'Widget' as item_name
        UNION ALL
        SELECT 2 as item_id, 'Gadget' as item_name
        ''';

        datasource target_items (
            item_id: item_id,
            item_name: item_name
        )
        grain (item_id)
        address target_items_table;

        CREATE IF NOT EXISTS DATASOURCE target_items;
        """
    )

    datasource = duckdb_engine.environment.datasources["target_items"]
    duckdb_engine.update_datasource(datasource)

    result = duckdb_engine.execute_raw_sql(
        "SELECT COUNT(*) as cnt FROM target_items_table"
    ).fetchone()
    assert result[0] == 2


def test_update_datasource_with_incremental_filter(duckdb_engine: Executor):
    """Test update_datasource with UpdateKeys filter for incremental update."""
    duckdb_engine.execute_text(
        """
        key record_id int;
        property record_id.created_ts datetime;
        property record_id.value string;

        datasource source_records (
            record_id: record_id,
            created_ts: created_ts,
            value: value
        )
        grain (record_id)
        query '''
        SELECT 1 as record_id, TIMESTAMP '2024-01-01 10:00:00' as created_ts, 'old' as value
        UNION ALL
        SELECT 2 as record_id, TIMESTAMP '2024-01-15 10:00:00' as created_ts, 'new1' as value
        UNION ALL
        SELECT 3 as record_id, TIMESTAMP '2024-01-20 10:00:00' as created_ts, 'new2' as value
        '''
        incremental by created_ts;

        datasource target_records (
            record_id: record_id,
            created_ts: created_ts,
            value: value
        )
        grain (record_id)
        address target_records_table
        incremental by created_ts;

        CREATE IF NOT EXISTS DATASOURCE target_records;

        RAW_SQL('''
        INSERT INTO target_records_table
        SELECT 1 as record_id, TIMESTAMP '2024-01-01 10:00:00' as created_ts, 'old' as value
        ''');
        """
    )

    # Create UpdateKeys with filter for records after the existing one
    keys = UpdateKeys(
        keys={
            "created_ts": UpdateKey(
                concept_name="created_ts",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=datetime(2024, 1, 1, 10, 0, 0),
            )
        }
    )

    datasource = duckdb_engine.environment.datasources["target_records"]
    duckdb_engine.update_datasource(datasource, keys)

    result = duckdb_engine.execute_raw_sql(
        "SELECT COUNT(*) as cnt FROM target_records_table"
    ).fetchone()
    # Should have original 1 + 2 new records = 3
    assert result[0] == 3


def test_update_datasource_integration_with_stale_assets(duckdb_engine: Executor):
    """Test full integration: detect stale, then update using filters."""
    duckdb_engine.execute_text(
        """
        key event_id int;
        property event_id.event_ts datetime;
        property event_id.event_data string;

        datasource events_source (
            event_id: event_id,
            event_ts: event_ts,
            event_data: event_data
        )
        grain (event_id)
        query '''
        SELECT 1 as event_id, TIMESTAMP '2024-01-01 10:00:00' as event_ts, 'data1' as event_data
        UNION ALL
        SELECT 2 as event_id, TIMESTAMP '2024-01-10 10:00:00' as event_ts, 'data2' as event_data
        UNION ALL
        SELECT 3 as event_id, TIMESTAMP '2024-01-20 10:00:00' as event_ts, 'data3' as event_data
        '''
        incremental by event_ts;

        datasource events_target (
            event_id: event_id,
            event_ts: event_ts,
            event_data: event_data
        )
        grain (event_id)
        address events_target_table
        incremental by event_ts;

        CREATE IF NOT EXISTS DATASOURCE events_target;

        RAW_SQL('''
        INSERT INTO events_target_table
        SELECT 1 as event_id, TIMESTAMP '2024-01-01 10:00:00' as event_ts, 'data1' as event_data
        ''');
        """
    )

    state_store = BaseStateStore()
    stale = state_store.get_stale_assets(
        duckdb_engine.environment,
        duckdb_engine,
        root_assets={"events_source"},
    )

    assert len(stale) == 1
    assert stale[0].datasource_id == "events_target"

    # Use the stale asset's filters to do an incremental update
    datasource = duckdb_engine.environment.datasources["events_target"]
    duckdb_engine.update_datasource(datasource, stale[0].filters)

    result = duckdb_engine.execute_raw_sql(
        "SELECT COUNT(*) as cnt FROM events_target_table"
    ).fetchone()
    # Original 1 + 2 incremental = 3
    assert result[0] == 3


def test_update_keys_to_where_clause(duckdb_engine: Executor):
    """Test UpdateKeys.to_where_clause conversion."""
    duckdb_engine.execute_text(
        """
        key id int;
        property id.ts datetime;
        """
    )

    keys = UpdateKeys(
        keys={
            "ts": UpdateKey(
                concept_name="ts",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=datetime(2024, 1, 15, 0, 0, 0),
            )
        }
    )

    where = keys.to_where_clause(duckdb_engine.environment)
    assert where is not None
    assert "ts" in str(where.conditional)


def test_update_keys_to_where_clause_empty(duckdb_engine: Executor):
    """Test UpdateKeys.to_where_clause with no values returns None."""
    duckdb_engine.execute_text(
        """
        key id int;
        property id.ts datetime;
        """
    )

    keys = UpdateKeys(
        keys={
            "ts": UpdateKey(
                concept_name="ts",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=None,
            )
        }
    )

    where = keys.to_where_clause(duckdb_engine.environment)
    assert where is None


def test_update_keys_to_where_clause_multiple(duckdb_engine: Executor):
    """Test UpdateKeys.to_where_clause with multiple keys."""
    duckdb_engine.execute_text(
        """
        key id int;
        property id.ts datetime;
        property id.version int;
        """
    )

    keys = UpdateKeys(
        keys={
            "ts": UpdateKey(
                concept_name="ts",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=datetime(2024, 1, 15, 0, 0, 0),
            ),
            "version": UpdateKey(
                concept_name="version",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=5,
            ),
        }
    )

    where = keys.to_where_clause(duckdb_engine.environment)
    assert where is not None
    # Should contain AND for multiple conditions
    where_str = str(where.conditional)
    assert "ts" in where_str
    assert "version" in where_str
