# from preql.compiler import compile
from pytest import raises

from preql.core.exceptions import InvalidSyntaxException
from preql.core.models import Select
from preql.core.query_processor import process_query
from preql.dialect.base import BaseDialect
from preql.parser import parse
from preql.hooks.query_debugger import DebuggingHook


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

    BaseDialect().compile_statement(
        process_query(test_environment, select, hooks=[DebuggingHook()])
    )


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


def test_window_functions(test_environment):
    declarations = """

    property prior_order_id <- lag order_id by order_id desc;
    property next_order_id <- lead order_id order by order_id asc;
    select
        order_id,
        prior_order_id,
        next_order_id
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


def test_math_functions(test_environment):
    from preql.hooks.query_debugger import DebuggingHook

    declarations = """
    
    
    property inflated_order_value<- multiply(revenue, 2);
    property fixed_order_value<- inflated_order_value / 2;
    property order_sub <- revenue - 2;
    property order_add <- revenue + 2;
    property order_nested <- revenue*2/2;
    property rounded <- round(revenue + 2.01,2);
    select
        order_id,
        inflated_order_value,
        fixed_order_value,
        order_sub,
        order_add,
        order_nested,
        rounded
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]
    x = BaseDialect().compile_statement(
        process_query(test_environment, select, hooks=[DebuggingHook()])
    )
    print(x)


def test_string_functions(test_environment):
    declarations = """
    property test_name <- concat(category_name, '_test');
    property upper_name <- upper(category_name);
    property lower_name <- lower(category_name);
    select
        test_name,
        upper_name,
        lower_name
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
