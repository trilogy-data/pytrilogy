from cProfile import Profile
from pstats import SortKey, Stats

from trilogy import BoundEnvironment, parse
from trilogy.core.enums import (
    ComparisonOperator,
    FunctionType,
    Purpose,
    WindowType,
)
from trilogy.core.functions import Count, CountDistinct, Max, Min

# from trilogy.compiler import compile
from trilogy.core.execute_models import (
    BoundColumnAssignment,
    BoundComparison,
    BoundConcept,
    BoundDatasource,
    DataType,
    BoundFilterItem,
    BoundFunction,
    Grain,
    BoundOrderItem,
    SelectStatement,
    BoundWhereClause,
    BoundWindowItem,
)
from trilogy.core.query_processor import process_query
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.sql_server import SqlServerDialect


def gen_environment():
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
        lineage=BoundFunction(
            arguments=[revenue],
            output_datatype=DataType.FLOAT,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.SUM,
        ),
    )
    product_id = BoundConcept(
        name="product_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )

    assert product_id.grain.components[0].name == "product_id"

    category_id = BoundConcept(
        name="category_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    category_name = BoundConcept(
        name="category_name",
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        grain=category_id,
        keys=[category_id],
    )

    category_name_length = BoundConcept(
        name="category_name_length",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        grain=category_id,
        lineage=BoundFunction(
            arguments=[category_name],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            operator=FunctionType.LENGTH,
        ),
    )

    category_name_length_sum = BoundConcept(
        name="category_name_length_sum",
        datatype=DataType.INTEGER,
        purpose=Purpose.METRIC,
        grain=category_id,
        lineage=BoundFunction(
            arguments=[category_name_length],
            output_datatype=DataType.INTEGER,
            output_purpose=Purpose.METRIC,
            operator=FunctionType.SUM,
        ),
    )

    product_revenue_rank = BoundConcept(
        name="product_revenue_rank",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=BoundWindowItem(
            type=WindowType.RANK,
            content=product_id,
            order_by=[
                BoundOrderItem(expr=total_revenue.with_grain(product_id), order="desc")
            ],
        ),
        grain=product_id,
    )
    product_revenue_rank_by_category = BoundConcept(
        name="product_revenue_rank_by_category",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        lineage=BoundWindowItem(
            type=WindowType.RANK,
            content=product_id,
            over=[category_id],
            order_by=[BoundOrderItem(expr=total_revenue, order="desc")],
        ),
    )

    products_with_revenue_over_50 = BoundConcept(
        name="products_with_revenue_over_50",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        lineage=BoundFilterItem(
            content=product_id,
            where=BoundWhereClause(
                conditional=BoundComparison(
                    left=total_revenue.with_grain(product_id),
                    operator=ComparisonOperator.GT,
                    right=50,
                )
            ),
        ),
    )
    test_revenue = BoundDatasource(
        name="revenue",
        columns=[
            BoundColumnAssignment(alias="revenue", concept=revenue),
            BoundColumnAssignment(alias="order_id", concept=order_id),
            BoundColumnAssignment(alias="product_id", concept=product_id),
            BoundColumnAssignment(alias="order_timestamp", concept=order_timestamp),
        ],
        address="tblRevenue",
        grain=Grain(components=[order_id]),
    )

    test_product = BoundDatasource(
        name="products",
        columns=[
            BoundColumnAssignment(alias="product_id", concept=product_id),
            BoundColumnAssignment(alias="category_id", concept=category_id),
        ],
        address="tblProducts",
        grain=Grain(components=[product_id]),
    )

    test_category = BoundDatasource(
        name="category",
        columns=[
            BoundColumnAssignment(alias="category_id", concept=category_id),
            BoundColumnAssignment(alias="category_name", concept=category_name),
        ],
        address="tblCategory",
        grain=Grain(components=[category_id]),
    )

    for item in [test_product, test_category, test_revenue]:
        env.add_datasource(item)

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
        # env.concepts[item.name] = item
    return env


TEST_DIALECTS = [BaseDialect(), BigqueryDialect(), DuckDBDialect(), SqlServerDialect()]


def date_functions():
    environment = gen_environment()
    declarations = """

    select
        order_id,
        date(order_timestamp) -> order_date,
        datetime(order_timestamp) -> order_timestamp_datetime,
        timestamp(order_timestamp) -> order_timestamp_dos,
        second(order_timestamp) -> order_second,
        minute(order_timestamp) -> order_minute,
        hour(order_timestamp) -> order_hour,
        day(order_timestamp) -> order_day,
        week(order_timestamp) -> order_week,
        month(order_timestamp) -> order_month,
        quarter(order_timestamp) -> order_quarter,
        year(order_timestamp) -> order_year,
        date_trunc(order_timestamp, month) -> order_month_trunc,
        date_part(order_timestamp, month) -> order_month_part,
        date_add(order_timestamp, month, 1) -> one_month_post_order,
    ;
    
    
        """
    env, parsed = parse(declarations, environment=environment)
    select: SelectStatement = parsed[-1]

    BigqueryDialect().compile_statement(process_query(environment, select))


if __name__ == "__main__":
    with Profile() as profile:
        date_functions()
        (Stats(profile).strip_dirs().sort_stats(SortKey.CALLS).print_stats(25))
