from pytest import fixture

from preql import Environment
from preql.core.enums import DataType, Purpose, FunctionType
from preql.core.env_processor import generate_graph
from preql.core.models import Concept, Datasource, ColumnAssignment, Function, Grain


@fixture(scope="session")
def test_environment():
    env = Environment()
    order_id = Concept(name="order_id", datatype=DataType.INTEGER, purpose=Purpose.KEY)
    revenue = Concept(name="revenue", datatype=DataType.FLOAT, purpose=Purpose.PROPERTY)

    """    operator: FunctionType
    arguments: List[Concept]
    output_datatype: DataType
    output_purpose: Purpose
    output_grain: "Grain"
    valid_inputs: Optional[Set[DataType]] = None"""
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

    category_id = Concept(
        name="category_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    category_name = Concept(
        name="category_name",
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        grain=category_id,
    )

    # total_revenue =

    test_revenue = Datasource(
        identifier="revenue",
        columns=[
            ColumnAssignment(alias="revenue", concept=revenue),
            ColumnAssignment(alias="order_id", concept=order_id),
            ColumnAssignment(alias="product_id", concept=product_id),
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

    for item in [category_id, category_name, total_revenue, revenue, product_id]:
        env.concepts[item.name] = item
    yield env


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
