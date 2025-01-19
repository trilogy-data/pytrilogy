from pytest import fixture

from trilogy.core.enums import (
    ComparisonOperator,
    FunctionType,
    Modifier,
    Purpose,
    WindowType,
    Derivation,
    Granularity
)
from trilogy.core.env_processor import generate_graph
from trilogy.core.functions import Count, CountDistinct, Max, Min
from trilogy.core.models.author import (
    AggregateWrapper,
    Comparison,
    Concept,
    FilterItem,
    Function,
    Grain,
    OrderItem,
    WhereClause,
    WindowItem,
)
from trilogy.core.models.core import (
    DataType,
)
from trilogy.core.models.datasource import ColumnAssignment, Datasource
from trilogy.core.models.environment import Environment


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
    constant_one = Concept(
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

    literal_array = Concept(
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

    unnest_literal_array = Concept(
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
            arguments=[category_name],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            operator=FunctionType.LENGTH,
        ),
    )

    product_revenue_rank = Concept(
        name="product_revenue_rank",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=WindowItem(
            type=WindowType.RANK,
            content=product_id,
            order_by=[OrderItem(expr=total_revenue, order="desc")],
        ),
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
        derivation = Derivation.FILTER
    )

    category_top_50_revenue_products = Concept(
        name="category_top_50_revenue_products",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=AggregateWrapper(
            function=Count([products_with_revenue_over_50], env), by=[category_id]
        ),
        grain=Grain(components=[category_id]),
        derivation = Derivation.AGGREGATE
    )

    category_products = Concept(
        name="category_products",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        lineage=AggregateWrapper(function=Count([product_id], env), by=[category_id]),
        derivation = Derivation.AGGREGATE
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
    for item in [test_product, test_category, test_revenue]:
        env.add_datasource(item)

    env.gen_concept_list_caches()
    yield env


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
