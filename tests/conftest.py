from pytest import fixture

from preql import Environment
from preql.core.enums import DataType, Purpose, FunctionType
from preql.core.env_processor import generate_graph
from preql.core.models import Concept, Datasource, ColumnAssignment, Function, Grain
from preql.constants import logger
from logging import StreamHandler, DEBUG


@fixture(scope="session")
def logger():
    logger.addHandler(StreamHandler())
    logger.setLevel(DEBUG)


@fixture(scope="session")
def test_environment():
    env = Environment()
    order_id = Concept(name="order_id", datatype=DataType.INTEGER, purpose=Purpose.KEY)

    order_timestamp = Concept(
        name="order_timestamp", datatype=DataType.TIMESTAMP, purpose=Purpose.PROPERTY
    )

    order_count = Concept(
        name="order_count",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Function(
            arguments=[order_id],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.COUNT,
        ),
    )

    distinct_order_count = Concept(
        name="distinct_order_count",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Function(
            arguments=[order_id],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.COUNT_DISTINCT,
        ),
    )

    max_order_id = Concept(
        name="max_order_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Function(
            arguments=[order_id],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.MAX,
        ),
    )

    min_order_id = Concept(
        name="min_order_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Function(
            arguments=[order_id],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.MIN,
        ),
    )

    revenue = Concept(name="revenue", datatype=DataType.FLOAT, purpose=Purpose.PROPERTY)

    total_revenue = Concept(
        name="total_revenue",
        datatype=DataType.FLOAT,
        purpose=Purpose.METRIC,
        lineage=Function(
            arguments=[revenue],
            output_datatype=DataType.FLOAT,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.SUM,
        ),
    )
    product_id = Concept(
        name="product_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )

    assert product_id.grain.components[0].name == "product_id"

    category_id = Concept(
        name="category_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    category_name = Concept(
        name="category_name",
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        grain=category_id,
    )

    category_name_length = Concept(
        name="category_name_length",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        grain=category_id,
        lineage=Function(
            arguments=[category_name],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            operator=FunctionType.LENGTH,
        ),
    )

    # total_revenue =

    test_revenue = Datasource(
        identifier="revenue",
        columns=[
            ColumnAssignment(alias="revenue", concept=revenue),
            ColumnAssignment(alias="order_id", concept=order_id),
            ColumnAssignment(alias="product_id", concept=product_id),
            ColumnAssignment(alias="order_timestamp", concept=order_timestamp),
        ],
        address="tblRevenue",
        grain=Grain(components=[order_id]),
    )

    test_product = Datasource(
        identifier="products",
        columns=[
            ColumnAssignment(alias="product_id", concept=product_id),
            ColumnAssignment(alias="category_id", concept=category_id),
        ],
        address="tblProducts",
    )

    test_category = Datasource(
        identifier="category",
        columns=[
            ColumnAssignment(alias="category_id", concept=category_id),
            ColumnAssignment(alias="category_name", concept=category_name),
        ],
        address="tblCategory",
    )

    for item in [test_product, test_category, test_revenue]:
        env.datasources[item.identifier] = item

    for item in [
        category_id,
        category_name,
        category_name_length,
        total_revenue,
        revenue,
        product_id,
        order_id,
        order_count,
        order_timestamp,
        distinct_order_count,
        min_order_id,
        max_order_id,
    ]:
        env.concepts[item.name] = item
    yield env


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
