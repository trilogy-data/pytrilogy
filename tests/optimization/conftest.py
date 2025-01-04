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
    DataType,
)

from trilogy.core.author_models import Concept, Function, OrderItem, WindowItem, Comparison, WhereClause, FilterItem, Datasource, ColumnAssignment, Grain
from trilogy.core.functions import Count, CountDistinct, Max, Min, FunctionFactory, create_function_derived_concept

@fixture(scope="session")
def test_environment():
    env = Environment()
    order_id = Concept(name="order_id", datatype=DataType.INTEGER, purpose=Purpose.KEY)

    order_timestamp = Concept(
        name="order_timestamp", datatype=DataType.TIMESTAMP, purpose=Purpose.PROPERTY
    )
    order_count = create_function_derived_concept(
        name="order_count",
        operator = FunctionType.COUNT,
        arguments = [order_id],
        environment=env,
    )


    distinct_order_count =  create_function_derived_concept(
        name="distinct_order_count",
        operator = FunctionType.COUNT_DISTINCT,
        arguments = [order_id],
          environment=env,
    )

    max_order_id = create_function_derived_concept(
        name="max_order_id",
        operator = FunctionType.MAX,
        arguments = [order_id],
          environment=env,
    )

    min_order_id = create_function_derived_concept(
        name="min_order_id",
        operator = FunctionType.MIN,
        arguments = [order_id],
          environment=env,
    )

    revenue = Concept(
        name="revenue",
        datatype=DataType.FLOAT,
        purpose=Purpose.PROPERTY,
        keys={order_id.address},
        grain=Grain(components=[order_id]),
    )

    total_revenue = create_function_derived_concept(
        name="total_revenue",
        operator = FunctionType.SUM,
        arguments = [revenue],
        environment=env 
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

    category_name_length = create_function_derived_concept(
        name="category_name_length",
        operator = FunctionType.LENGTH,
        arguments = [category_name],
        environment=env,
    )

    category_name_length_sum = create_function_derived_concept(
        name="category_name_length_sum",
        operator = FunctionType.SUM,
        arguments = [category_name_length],
          environment=env,
    )

    product_revenue_rank = Concept(
        name="product_revenue_rank",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=WindowItem(
            type=WindowType.RANK,
            content=product_id,
            order_by=[
                OrderItem(expr=total_revenue.with_grain(product_id).reference, order="desc")
            ],
        ),
        grain=product_id,
    )
    product_revenue_rank_by_category = Concept(
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

    products_with_revenue_over_50 = Concept(
        name="products_with_revenue_over_50",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
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

        # env.concepts[item.name] = item
    yield env


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
