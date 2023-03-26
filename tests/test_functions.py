# from preql.compiler import compile
from pytest import raises

from preql.core.exceptions import InvalidSyntaxException
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


def test_date_functions(test_environment):
    declarations = """

    select
        order_id,
        date(order_timestamp) -> order_date,
        timestamp(order_timestamp) -> order_timestamp_2,
        second(order_timestamp) -> order_second,
        minute(order_timestamp) -> order_minute,
        hour(order_timestamp) -> order_hour,
        day(order_timestamp) -> order_day,
        month(order_timestamp) -> order_month,
        year(order_timestamp) -> order_year,
    ;
    
    
        """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    x = BaseDialect().compile_statement(process_query(test_environment, select))


def test_bad_cast(test_environment):
    declarations = """

    select
        order_id,
        date(order_id) -> order_id_date,
    ;"""
    with raises(InvalidSyntaxException):
        env, parsed = parse(declarations, environment=test_environment)


def test_explicit_cast(test_environment):
    declarations = """
    property _str_order_id <- cast(order_id as string);
    select
        order_id,
        _str_order_id
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
