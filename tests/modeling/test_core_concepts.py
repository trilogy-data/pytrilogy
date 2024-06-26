from trilogy.core.models import Environment
from trilogy.core.enums import Purpose
from trilogy import parse, Executor
from trilogy.core.processing.node_generators import gen_select_node
from trilogy.core.env_processor import generate_graph
import pytest


def test_key_assignments(test_environment: Environment):
    # test keys
    order_id = test_environment.concepts["order_id"]
    store_id = test_environment.concepts["store_id"]
    product_id = test_environment.concepts["product_id"]

    for key in [order_id, store_id, product_id]:
        assert key.purpose == Purpose.KEY
        assert key.grain.components == [key]
    # test_environment.concepts[]


def test_property_assignments(test_environment: Environment):
    # test keys
    store_id = test_environment.concepts["store_id"]
    product_id = test_environment.concepts["product_id"]
    store_name = test_environment.concepts["store_name"]
    product_name = test_environment.concepts["product_name"]

    assert store_name.purpose == Purpose.PROPERTY
    assert product_name.purpose == Purpose.PROPERTY

    assert store_name.keys == (store_id,)
    assert store_name.grain.components == [store_id]
    assert product_name.keys == (product_id,)
    assert product_name.grain.components == [product_id]


def test_auto_property_assignments(test_environment: Environment):
    # test keys
    store_id = test_environment.concepts["store_id"]
    store_name = test_environment.concepts["store_name"]
    upper_store_name = test_environment.concepts["upper_store_name"]
    upper_store_name_2 = test_environment.concepts["upper_store_name_2"]

    assert upper_store_name.lineage.concept_arguments == [store_name]
    assert upper_store_name_2.lineage.output_keys == [store_id]

    for candidate in [store_name, upper_store_name, upper_store_name_2]:
        assert candidate.purpose == Purpose.PROPERTY
        assert candidate.keys == (
            store_id,
        ), f"keys for {candidate.address}: {candidate.keys} should be store_id"
        assert {x.address for x in candidate.grain.components} == set(
            [store_id.address]
        ), f"grain for {candidate.address}: {candidate.keys} should be store_id"


def test_metric_assignments(test_environment: Environment):
    # test keys
    store_id = test_environment.concepts["store_id"]
    store_order_count = test_environment.concepts["store_order_count"]
    store_order_count_2 = test_environment.concepts["store_order_count_2"]

    for candidate in [store_order_count, store_order_count_2]:
        assert candidate.purpose == Purpose.METRIC
        assert candidate.keys == (store_id,)
        assert candidate.grain.components == [store_id]


def test_source_outputs(test_environment: Environment, test_executor: Executor):
    order_ds = test_environment.datasources["orders"]
    for col in order_ds.columns:
        if col.alias == "order_id":
            assert col.is_complete
        elif col.alias == "store_id":
            assert not col.is_complete
        elif col.alias == "product_id":
            assert not col.is_complete

    x = gen_select_node(
        test_environment.concepts["store_id"],
        local_optional=[test_environment.concepts["order_id"]],
        environment=test_environment,
        g=generate_graph(test_environment),
        depth=0,
        accept_partial=True,
    )

    found = False
    for con in x.partial_concepts:
        if con.address == test_environment.concepts["store_id"].address:
            found = True
    assert found

    resolved = x.resolve()
    found = False
    for con in resolved.partial_concepts:
        if con.address == test_environment.concepts["store_id"].address:
            found = True
    assert found


def test_statement_grains(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    store_id, 
    store_name,
    count(order_id) as store_order_count_3,
    store_order_count,
    store_order_count_2
;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert results[0] == (1, "store1", 2, 2, 2)
    assert results[1] == (2, "store2", 2, 2, 2)
    assert results[2] == (3, "store3", 0, 0, 0)


def test_join_grain(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
SELECT
    store_id, 
    product_id,
    order_id,
;"""
    _, statements = parse(test_select, test_environment)
    statement = statements[-1]
    assert set(statement.grain.components) == set(
        [
            test_environment.concepts["store_id"],
            test_environment.concepts["product_id"],
            test_environment.concepts["order_id"],
        ]
    )

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 5


def test_filter_grain(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """

auto even_order_id <- filter order_id where (order_id % 2) = 0;
SELECT
    order_id,
    even_order_id
;"""
    _, statements = parse(test_select, test_environment)
    statement = statements[-1]
    assert set([x.address for x in statement.grain.components]) == {
        "local.even_order_id",
        "local.order_id",
    }
    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 4

    test_select = """

SELECT
    even_order_id
;"""
    _, statements = parse(test_select, test_environment)
    statement = statements[-1]
    assert set([x.address for x in statement.grain.components]) == {
        "local.even_order_id"
    }

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 3


def test_datasource_properties(test_environment: Environment, test_executor: Executor):
    cols = test_environment.datasources["orders"].columns

    for col in cols:
        if col.concept.name == "store_id":
            assert not col.is_complete
        if col.concept.name == "product_id":
            assert not col.is_complete

    store = test_environment.datasources["stores"]

    assert store.grain == [test_environment.concepts["store_id"]]


def test_filter_grain_different(test_environment: Environment, test_executor: Executor):
    test_select = """

    auto even_order_store_id <- filter store_id where (order_id % 2) = 0;
    SELECT
        store_id,
        order_id,
        even_order_store_id
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[-1].fetchall())
    assert len(results) == 5


def test_inline_source_derivation(
    test_environment: Environment, test_executor: Executor
):
    test_select = """

    SELECT
        order_year,
        count(order_id) as order_count
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    for row in results:
        assert row.order_year == 1992


@pytest.mark.xfail
def test_filtered_project(test_environment: Environment, test_executor: Executor):
    test_select = """

    SELECT
        even_product_name
    ;"""
    _, statements = parse(test_select, test_environment)

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
