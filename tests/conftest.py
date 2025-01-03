from pytest import fixture

from trilogy import Environment
from trilogy.core.enums import (
    ComparisonOperator,
    FunctionType,
    Purpose,
    WindowType,
)
from trilogy.core.env_processor import generate_graph
from trilogy.core.functions import Count, CountDistinct, Max, Min
from trilogy.core.execute_models import (
    BoundColumnAssignment,
    BoundComparison,
    BoundConcept,
    BoundDatasource,
    DataType,
    BoundFilterItem,
    BoundFunction,

    BoundOrderItem,
    BoundWhereClause,
    BoundWindowItem,
    BoundFunction,
)
from trilogy.core.author_models import Concept, Function, OrderItem, WindowItem, Comparison, WhereClause, FilterItem, Datasource, ColumnAssignment, Grain


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
        lineage=Count([order_id], env),
    )

    distinct_order_count = Concept(
        name="distinct_order_count",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=CountDistinct([order_id], env),
    )

    max_order_id = Concept(
        name="max_order_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Max([order_id], env),
    )

    min_order_id = Concept(
        name="min_order_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Min([order_id], env),
    )

    revenue = Concept(
        name="revenue",
        datatype=DataType.FLOAT,
        purpose=Purpose.PROPERTY,
        keys={order_id.address},
        grain=Grain(components=[order_id]),
    )

    total_revenue = Concept(
        name="total_revenue",
        datatype=DataType.FLOAT,
        purpose=Purpose.METRIC,
        lineage=Function(
            arguments=[revenue.reference],
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
        keys={category_id.address},
    )

    category_name_length = Concept(
        name="category_name_length",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        grain=category_id,
        lineage=Function(
            arguments=[category_name.reference],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            operator=FunctionType.LENGTH,
        ),
        keys={category_id.address},
    )

    category_name_length_sum = Concept(
        name="category_name_length_sum",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        grain=category_id,
        lineage=Function(
            arguments=[category_name_length.reference],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.SUM,
        ),
    )
    rev_by_product = total_revenue.with_grain(product_id, name="rev_by_product")
    env.add_concept(rev_by_product)

    product_revenue_rank = Concept(
        name="product_revenue_rank",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=WindowItem(
            type=WindowType.RANK,
            content=product_id.reference,
            order_by=[
                OrderItem(expr = rev_by_product.reference, order="desc")
            ],
        ),
        grain=product_id,
        keys={product_id.address},
    )
    product_revenue_rank_by_category = Concept(
        name="product_revenue_rank_by_category",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=WindowItem(
            type=WindowType.RANK,
            content=product_id.reference,
            over=[category_id],
            order_by=[OrderItem(expr=total_revenue, order="desc")],
        ),
    )

    products_with_revenue_over_50 = Concept(
        name="products_with_revenue_over_50",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        lineage=FilterItem(
            content=product_id,
            where=WhereClause(
                conditional=Comparison(
                    left=total_revenue.with_grain(product_id).reference,
                    operator=ComparisonOperator.GT,
                    right=50,
                )
            ),
        ),
    )
    test_revenue = Datasource(
        name="revenue",
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
        name="products",
        columns=[
            ColumnAssignment(alias="product_id", concept=product_id),
            ColumnAssignment(alias="category_id", concept=category_id),
        ],
        address="tblProducts",
        grain=Grain(components=[product_id]),
    )

    test_category = Datasource(
        name="category",
        columns=[
            ColumnAssignment(alias="category_id", concept=category_id),
            ColumnAssignment(alias="category_name", concept=category_name),
        ],
        address="tblCategory",
        grain=Grain(components=[category_id]),
    )



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
        product_revenue_rank,
        product_revenue_rank_by_category,
        products_with_revenue_over_50,
        category_name_length_sum,
    ]:
        env.add_concept(item)

    for item in [test_product, test_category, test_revenue]:
        env.add_datasource(item)

    yield env


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
