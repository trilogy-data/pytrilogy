from pytest import fixture

from trilogy import BoundEnvironment
from trilogy.core.enums import (
    ComparisonOperator,
    FunctionType,
    Modifier,
    Purpose,
    WindowType,
)
from trilogy.core.env_processor import generate_graph
from trilogy.core.functions import Count, CountDistinct, Max, Min
from trilogy.core.execute_models import (
    AggregateWrapper,
    ColumnAssignment,
    Comparison,
    BoundConcept,
    Datasource,
    DataType,
    FilterItem,
    Function,
    Grain,
    OrderItem,
    WhereClause,
    WindowItem,
)


@fixture(scope="session")
def test_environment():
    env = BoundEnvironment()
    order_id = BoundConcept(name="order_id", datatype=DataType.INTEGER, purpose=Purpose.KEY)

    order_timestamp = BoundConcept(
        name="order_timestamp", datatype=DataType.TIMESTAMP, purpose=Purpose.PROPERTY
    )

    order_count = BoundConcept(
        name="order_count",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Count([order_id]),
    )

    distinct_order_count = BoundConcept(
        name="distinct_order_count",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=CountDistinct([order_id]),
    )

    max_order_id = BoundConcept(
        name="max_order_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Max([order_id]),
    )

    min_order_id = BoundConcept(
        name="min_order_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=Min([order_id]),
    )

    revenue = BoundConcept(name="revenue", datatype=DataType.FLOAT, purpose=Purpose.PROPERTY)

    total_revenue = BoundConcept(
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
    product_id = BoundConcept(
        name="product_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    constant_one = BoundConcept(
        name="constant_one",
        datatype=DataType.INTEGER,
        purpose=Purpose.CONSTANT,
        lineage=Function(
            arguments=[1],
            output_datatype=DataType.ARRAY,
            output_purpose=Purpose.CONSTANT,
            operator=FunctionType.CONSTANT,
        ),
    )

    literal_array = BoundConcept(
        name="literal_array",
        datatype=DataType.ARRAY,
        purpose=Purpose.CONSTANT,
        lineage=Function(
            arguments=[[1, 2, 3]],
            output_datatype=DataType.ARRAY,
            output_purpose=Purpose.CONSTANT,
            operator=FunctionType.CONSTANT,
        ),
    )

    unnest_literal_array = BoundConcept(
        name="unnest_literal_array",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        lineage=Function(
            arguments=[literal_array],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.KEY,
            operator=FunctionType.UNNEST,
        ),
    )

    category_id = BoundConcept(
        name="category_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    category_name = BoundConcept(
        name="category_name",
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        grain=category_id,
        keys={category_id.address},
    )

    category_name_length = BoundConcept(
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

    product_revenue_rank = BoundConcept(
        name="product_revenue_rank",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=WindowItem(
            type=WindowType.RANK,
            content=product_id,
            order_by=[OrderItem(expr=total_revenue, order="desc")],
        ),
    )
    product_revenue_rank_by_category = BoundConcept(
        name="product_revenue_rank_by_category",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=WindowItem(
            type=WindowType.RANK,
            content=product_id,
            over=[category_id],
            order_by=[OrderItem(expr=total_revenue, order="desc")],
        ),
    )

    products_with_revenue_over_50 = BoundConcept(
        name="products_with_revenue_over_50",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=FilterItem(
            content=product_id,
            where=WhereClause(
                conditional=Comparison(
                    left=total_revenue.with_grain(product_id),
                    operator=ComparisonOperator.GT,
                    right=50,
                )
            ),
        ),
        grain=product_id,
    )

    category_top_50_revenue_products = BoundConcept(
        name="category_top_50_revenue_products",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=AggregateWrapper(
            function=Count([products_with_revenue_over_50]), by=[category_id]
        ),
        grain=Grain(components=[category_id]),
    )

    category_products = BoundConcept(
        name="category_products",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=AggregateWrapper(function=Count([product_id]), by=[category_id]),
    )

    test_revenue = Datasource(
        name="revenue",
        columns=[
            ColumnAssignment(alias="revenue", concept=revenue),
            ColumnAssignment(alias="order_id", concept=order_id),
            ColumnAssignment(
                alias="product_id", concept=product_id, modifiers=[Modifier.PARTIAL]
            ),
            ColumnAssignment(alias="order_timestamp", concept=order_timestamp),
        ],
        address="tblRevenue",
        grain=Grain(components=[order_id]),
    )

    test_product = Datasource(
        name="products",
        columns=[
            ColumnAssignment(alias="product_id", concept=product_id),
            ColumnAssignment(
                alias="category_id", concept=category_id, modifiers=[Modifier.PARTIAL]
            ),
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

    for item in [test_product, test_category, test_revenue]:
        env.add_datasource(item)

    for item in [
        constant_one,
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
        category_top_50_revenue_products,
        category_products,
        literal_array,
        unnest_literal_array,
    ]:
        env.add_concept(item)
        # env.concepts[item.name] = item
    yield env


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
