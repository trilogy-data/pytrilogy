from trilogy import Executor
from trilogy.execution.state.state_store import (
    BaseStateStore,
    WatermarkType,
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
    assert watermarks.keys["update_time"].type == WatermarkType.UPDATE_TIME
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
        incremental_by (order_date)
        query '''
        SELECT 1 as order_id, 100.0 as amount, '2024-01-01 10:00:00' as order_date
        UNION ALL
        SELECT 2 as order_id, 200.0 as amount, '2024-01-05 11:00:00' as order_date
        UNION ALL
        SELECT 3 as order_id, 150.0 as amount, '2024-01-10 12:00:00' as order_date
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["orders"]
    watermarks = get_incremental_key_watermarks(datasource, duckdb_engine)

    assert "order_date" in watermarks.keys
    assert watermarks.keys["order_date"].type == WatermarkType.INCREMENTAL_KEY
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
    assert watermarks.keys["product_id"].type == WatermarkType.KEY_HASH
    assert watermarks.keys["store_id"].type == WatermarkType.KEY_HASH
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
        incremental_by (timestamp)
        query '''
        SELECT 1 as transaction_id, '2024-01-01 10:00:00' as timestamp, 50.0 as amount
        UNION ALL
        SELECT 2 as transaction_id, '2024-01-02 11:00:00' as timestamp, 75.0 as amount
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["transactions"]
    state_store = BaseStateStore()

    watermarks = state_store.watermark_root_asset(datasource, duckdb_engine)

    assert watermarks is not None
    assert "timestamp" in watermarks.keys
    assert watermarks.keys["timestamp"].type == WatermarkType.INCREMENTAL_KEY

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

    watermarks = state_store.watermark_root_asset(datasource, duckdb_engine)

    assert watermarks is not None
    assert "customer_id" in watermarks.keys
    assert watermarks.keys["customer_id"].type == WatermarkType.KEY_HASH


def test_base_state_store_update_time(duckdb_engine: Executor):
    duckdb_engine.execute_text(
        """
        property event_type string;
        property event_count int;

        datasource events (
            event_type: event_type,
            event_count: event_count
        )
        grain ()
        query '''
        SELECT 'login' as event_type, 100 as event_count
        UNION ALL
        SELECT 'logout' as event_type, 95 as event_count
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["events"]
    state_store = BaseStateStore()

    watermarks = state_store.watermark_root_asset(datasource, duckdb_engine)

    assert watermarks is not None
    assert "update_time" in watermarks.keys
    assert watermarks.keys["update_time"].type == WatermarkType.UPDATE_TIME


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
        property metric_name string;
        property metric_value float;

        datasource metrics (
            metric_name: metric_name,
            metric_value: metric_value
        )
        grain ()
        query '''
        SELECT 'cpu_usage' as metric_name, 75.5 as metric_value
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["metrics"]
    watermarks = get_unique_key_hash_watermarks(datasource, duckdb_engine)

    assert watermarks.keys == {}


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
        incremental_by (updated_at, version)
        query '''
        SELECT 1 as record_id, '2024-01-01 10:00:00' as updated_at, 1 as version
        UNION ALL
        SELECT 2 as record_id, '2024-01-02 11:00:00' as updated_at, 2 as version
        UNION ALL
        SELECT 3 as record_id, '2024-01-03 12:00:00' as updated_at, 3 as version
        ''';
        """
    )

    datasource = duckdb_engine.environment.datasources["versioned_records"]
    watermarks = get_incremental_key_watermarks(datasource, duckdb_engine)

    assert "updated_at" in watermarks.keys
    assert "version" in watermarks.keys
    assert watermarks.keys["updated_at"].type == WatermarkType.INCREMENTAL_KEY
    assert watermarks.keys["version"].type == WatermarkType.INCREMENTAL_KEY
