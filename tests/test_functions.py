# from preql.compiler import compile
from preql.core.models import Select
from preql.core.query_processor import process_query
from preql.dialect.base import BaseDialect
from preql.parser import parse


def test_functions(test_environment):
    declarations = """

select
    total_revenue,
    min_order_id,
    max_order_id,
    order_count,
    distinct_order_count
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    BaseDialect().compile_statement(process_query(test_environment, select))


def test_wrapped_property_functions(test_environment):
    declarations = """

select
    product_id,
    avg(category_name_length) ->average_category_name_length
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    x = BaseDialect().compile_statement(process_query(test_environment, select))
    print(x)
