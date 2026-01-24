from pathlib import Path

import pytest

from trilogy import Dialects, Environment


def setup_environment():
    """Setup test environment"""
    env = Environment(working_path=Path(__file__).parent)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    return env, exec


def test_total_summary():
    """Test overall totals resolve to total_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    sum(order_value) as total_value,
    count(order_id) as order_count,
    count_distinct(customer_id) as customer_count,
    count_distinct(product_id) as product_count
;
"""
    )[-1]
    assert (
        "total_summary" in generated
    ), f"Expected total_summary table, got: {generated}"


def test_customer_aggregates():
    """Test customer-level aggregates resolve to customer_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    sum(order_value) as customer_revenue,
    count(order_id) as customer_order_count
;
"""
    )[-1]
    assert (
        "customer_summary" in generated
    ), f"Expected customer_summary table, got: {generated}"


def test_product_aggregates():
    """Test product-level aggregates resolve to product_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    product_id,
    sum(order_value) as product_revenue,
    count(order_id) as product_order_count,
    count_distinct(customer_id) as product_customer_count
;
"""
    )[-1]
    assert (
        "product_summary" in generated
    ), f"Expected product_summary table, got: {generated}"


def test_daily_aggregates():
    """Test daily aggregates resolve to daily_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    order_date,
    sum(order_value) as daily_revenue,
    count(order_id) as daily_order_count,
    count_distinct(customer_id) as daily_customer_count,
    count_distinct(product_id) as daily_product_count
;
"""
    )[-1]
    assert (
        "daily_summary" in generated
    ), f"Expected daily_summary table, got: {generated}"


def test_customer_product_aggregates():
    """Test customer-product aggregates resolve to customer_product_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    product_id,
    sum(order_value) as customer_product_revenue,
    count(order_id) as customer_product_order_count
;
"""
    )[-1]
    assert (
        "customer_product_summary" in generated
    ), f"Expected customer_product_summary table, got: {generated}"


def test_customer_daily_aggregates():
    """Test customer-daily aggregates resolve to customer_daily_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    order_date,
    sum(order_value) as customer_daily_revenue,
    count(order_id) as customer_daily_order_count
;
"""
    )[-1]
    assert (
        "customer_daily_summary" in generated
    ), f"Expected customer_daily_summary table, got: {generated}"


def test_product_daily_aggregates():
    """Test product-daily aggregates resolve to product_daily_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    product_id,
    order_date,
    sum(order_value) as product_daily_revenue,
    count(order_id) as product_daily_order_count,
    count_distinct(customer_id) as product_daily_customer_count
;
"""
    )[-1]
    assert (
        "product_daily_summary" in generated
    ), f"Expected product_daily_summary table, got: {generated}"


def test_customer_product_daily_aggregates():
    """Test customer-product-daily aggregates resolve to customer_product_daily_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    product_id,
    order_date,
    sum(order_value) as customer_product_daily_revenue,
    count(order_id) as customer_product_daily_order_count
;
"""
    )[-1]
    assert (
        "customer_product_daily_summary" in generated
    ), f"Expected customer_product_daily_summary table, got: {generated}"


def test_mouse_product_filter():
    """Test filtered aggregate for mouse product resolves to mouse_product_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    product_id,
    sum(order_value) as product_revenue,
    count(order_id) as product_order_count,
    count_distinct(customer_id) as product_customer_count
WHERE product_id = 202
;
"""
    )[-1]
    assert (
        "mouse_product_summary" in generated
    ), f"Expected mouse_product_summary table, got: {generated}"


def test_mouse_customer_filter():
    """Test filtered customer-product aggregate for mouse resolves to mouse_customer_summary table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    product_id,
    sum(order_value) as customer_product_revenue,
    count(order_id) as customer_product_order_count
WHERE product_id = 202
;
"""
    )[-1]
    assert (
        "mouse_customer_summary" in generated
    ), f"Expected mouse_customer_summary table, got: {generated}"


def test_high_value_customer_filter():
    """Test filtered customer aggregate for high-value customers resolves to high_value_customers table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    sum(order_value) as customer_revenue2,
    count(order_id) as customer_order_count
WHERE customer_revenue > 100
;
"""
    )[-1]
    assert (
        "high_value_customers" in generated
    ), f"Expected high_value_customers table, got: {generated}"


@pytest.mark.skip(reason="Need to implement complete detection for canonical types")
def test_high_value_customer_filter_two():
    """Test filtered customer aggregate for high-value customers resolves to high_value_customers table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    sum(order_value) as customer_revenue2,
    count(order_id) as customer_order_count
WHERE sum(order_value) > 100
;
"""
    )[-1]
    assert (
        "high_value_customers" in generated
    ), f"Expected high_value_customers table, got: {generated}"


def test_base_orders_table():
    """Test base query without aggregations resolves to orders table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    order_value
;
"""
    )[-1]
    assert "orders" in generated, f"Expected orders table, got: {generated}"


def test_customer_dimension():
    """Test customer dimension query resolves to customers table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    customer_name
;
"""
    )[-1]
    assert "customers" in generated, f"Expected customers table, got: {generated}"


def test_product_dimension():
    """Test product dimension query resolves to products table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    product_id,
    product_name
;
"""
    )[-1]
    assert "products" in generated, f"Expected products table, got: {generated}"


def test_mixed_aggregation_and_dimension():
    """Test query mixing aggregation and dimension data"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    customer_name,
    sum(order_value) as customer_revenue,
    count(order_id) as customer_order_count
;
"""
    )[-1]
    # Should use customer_summary for aggregates and join with customers for name
    assert (
        "customer_summary" in generated
    ), f"Expected customer tables, got: {generated}"


def test_partial_aggregation_match():
    """Test that partial aggregation matches still resolve to appropriate table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    sum(order_value) as customer_revenue
   # Missing customer_order_count that's in customer_summary
;
"""
    )[-1]
    assert (
        "customer_summary" in generated or "orders" in generated
    ), f"Expected customer_summary or orders table, got: {generated}"


def test_aggregate_with_additional_filter():
    """Test aggregation with additional filter that doesn't have a dedicated table"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    sum(order_value) as customer_revenue,
    count(order_id) as customer_order_count
WHERE order_date > '2024-01-15'::date
;
"""
    )[-1]
    # Should fall back to base table or filtered customer_summary
    assert (
        "customer_summary" in generated or "orders" in generated
    ), f"Expected customer_summary or orders table, got: {generated}"


def test_cross_dimensional_aggregation():
    """Test aggregation across different dimensions"""
    env, exec = setup_environment()
    from trilogy.core.models.build import Factory, generate_concept_name
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    _, statements = exec.environment.parse(
        """
import aggregate_testing;
SELECT
    customer_id,
    product_id,
    sum(order_value) as total_revenue
;
"""
    )
    generated = exec.generate_sql(statements[-1])[-1]
    # Should use appropriate table or fall back to base
    build_statement = Factory(
        environment=env,
    ).build(statements[-1].as_lineage(env))
    build_env = env.materialize_for_select(
        local_concepts=build_statement.local_concepts
    )
    # assert build_env.concepts['local.total_revenue'].canonical_address == build_env.concepts['customer_product_revenue'].canonical_address

    assert generate_concept_name(
        build_env.concepts["local.total_revenue"].lineage
    ) == generate_concept_name(build_env.concepts["customer_product_revenue"].lineage)
    assert any(
        table in generated for table in ["customer_product_summary"]
    ), f"Expected appropriate table, got: {generated}"


@pytest.mark.skip(reason="Stretch: detect when we can use partial agg for full agg")
def test_cross_dimensional_aggregation_one_key_only():
    """Test aggregation across different dimensions"""
    env, exec = setup_environment()
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_id,
    sum(order_value) as total_revenue
WHERE product_id in (201, 202)
;
"""
    )[-1]
    # Should use appropriate table or fall back to base
    assert any(
        table in generated for table in ["customer_product_summary"]
    ), f"Expected appropriate table, got: {generated}"


def test_temporal_aggregation():
    """Test temporal aggregation patterns"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    order_date,
    sum(order_value) as daily_revenue
WHERE order_date between '2024-01-15'::date and '2024-01-17'::date
;
"""
    )[-1]
    assert (
        "orders" in generated
    ), f"Expected daily_summary or orders table, got: {generated}"


def test_complex_multi_dimensional_query():
    """Test complex query spanning multiple dimensions"""
    env, exec = setup_environment()
    generated = exec.generate_sql(
        """
import aggregate_testing;
SELECT
    customer_name as customer_name,
    product_name as product_name,
    sum(order_value) as total_spent,
    count(order_id) as order_count,
    max(order_date) as last_order_date
;
"""
    )[-1]
    # Should involve multiple tables
    assert any(
        table in generated
        for table in ["customer_product_summary", "customers", "products", "orders"]
    ), f"Expected multiple tables, got: {generated}"
