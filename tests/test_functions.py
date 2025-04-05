# from trilogy.compiler import compile
from datetime import date, datetime
from logging import INFO

from pytest import raises

from trilogy import Dialects
from trilogy.constants import logger
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.core import DataType, ListType
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.parser import parse

logger.setLevel(INFO)

TEST_DIALECTS = [
    BaseDialect(),
    BigqueryDialect(),
    DuckDBDialect(),
    SqlServerDialect(),
    SnowflakeDialect(),
]


def test_typing():
    x = ListType(type=DataType.INTEGER)

    assert x in set([x])


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
    select: SelectStatement = parsed[-1]

    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select, hooks=[]))


def test_wrapped_property_functions(test_environment):
    declarations = """

select
    product_id,
    avg(category_name_length) ->average_category_name_length
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


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
    select: SelectStatement = parsed[-1]

    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_window_datatype(test_environment):
    declarations = """

    auto category_rank <- rank category_name order by count(order_id) desc;

        """
    env, parsed = parse(declarations, environment=test_environment)
    assert env.concepts["category_rank"].datatype == DataType.INTEGER


def test_date_functions(test_environment):
    declarations = """

    select
        order_id,
        order_timestamp,
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
        date_add(order_timestamp, month, 1) -> one_month_post_order,
        date_trunc(order_timestamp, day) -> order_day_trunc,
        date_trunc(order_timestamp, year) -> order_year_trunc,
        date_trunc(order_timestamp, hour) -> order_hour_trunc,
        date_trunc(order_timestamp, minute) -> order_minute_trunc,
        date_trunc(order_timestamp, second) -> order_second_trunc,
        date_part(order_timestamp, month) -> order_month_part,
        date_part(order_timestamp, day) -> order_day_part,
        date_part(order_timestamp, year) -> order_year_part,
        date_part(order_timestamp, hour) -> order_hour_part,
        date_part(order_timestamp, minute) -> order_minute_part,
        date_part(order_timestamp, second) -> order_second_part,
        date_part(order_timestamp, quarter) -> order_quarter_part,
        date_part(order_timestamp, week) -> order_week_part,
        date_part(order_timestamp, day_of_week) -> order_day_of_week_part,
    ;
    
    
        """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


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
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_literal_cast(test_environment):
    declarations = """
select
    '1'::int -> one,
    '1'::float -> one_float,
    '1'::string -> one_string,
    '2024-01-01'::date -> one_date,
    '2024-01-01 01:01:01'::datetime -> one_datetime,
    'true'::bool -> one_bool,
    ;"""
    env, parsed = parse(declarations, environment=test_environment)

    select: SelectStatement = parsed[-1]
    z = (
        Dialects.DUCK_DB.default_executor(environment=test_environment)
        .execute_query(parsed[-1])
        .fetchall()
    )
    assert z[0].one == 1
    assert z[0].one_float == 1.0
    assert z[0].one_string == "1"
    assert z[0].one_date == date(year=2024, month=1, day=1)
    assert z[0].one_datetime == datetime(
        year=2024, month=1, day=1, hour=1, minute=1, second=1
    )
    assert z[0].one_bool
    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_math_functions(test_environment):
    declarations = """
    
    
    property inflated_order_value<- multiply(revenue, 2);
    property fixed_order_value<- inflated_order_value / 2;
    property order_sub <- revenue - 2;
    property order_add <- revenue + 2;
    property order_id.order_nested <- revenue * 2/2;
    property order_id.rounded <- round(revenue + 2.01,2);
    select
        order_id,
        inflated_order_value,
        order_nested,
        fixed_order_value,
        order_sub,
        order_add,
        rounded
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_string_functions(test_environment):
    declarations = """
    property test_name <- concat(category_name, '_test');
    property upper_name <- upper(category_name);
    property lower_name <- lower(category_name);
    property substring_name <- substring(category_name, 1, 3);
    property strpos_name <- strpos(category_name, 'a');
    property like_name <- like(category_name, 'a%');
    property like_alt <- category_name like 'a%';

    select
        test_name,
        upper_name,
        lower_name,
        substring_name,
        strpos_name,
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_case_function(test_environment):
    declarations = """
    property test_upper_case <- CASE WHEN category_name = upper(category_name) then True else False END;
    select
        category_name,
        test_upper_case
    ;"""
    env, parsed = parse(declarations, environment=test_environment)

    assert (
        test_environment.concepts["category_name"]
        in test_environment.concepts["test_upper_case"].lineage.concept_arguments
    )
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        compiled = dialect.compile_statement(process_query(test_environment, select))
        assert "CASE" in compiled
        assert "ELSE" in compiled
        assert "END" in compiled
        assert test_environment.concepts["test_upper_case"].datatype == DataType.BOOL


def test_case_like_function(test_environment):
    declarations = """
    property test_like <- CASE WHEN category_name like '%abc%' then True else False END;
    select
        category_name,
        test_like
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    assert (
        test_environment.concepts["category_name"]
        in test_environment.concepts["test_like"].lineage.concept_arguments
    )
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        compiled = dialect.compile_statement(process_query(test_environment, select))
        assert "CASE" in compiled
        assert "ELSE" in compiled
        assert "END" in compiled
        assert test_environment.concepts["test_like"].datatype == DataType.BOOL


def test_split_and_index_function(test_environment):
    declarations = """
    constant test_string <- 'abc_def';

    
    select
        split(test_string, '_')->split_string,
        split(test_string, '_')[0] -> first_element,
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_coalesce(test_environment):
    declarations = """
    constant test_string <- 'abc_def';

    
    select
        coalesce(test_string, 'test')->coalesce_string,
        coalesce(null, 'test')->coalesce_null,
        coalesce(null, null, test_string)->coalesce_null_null,
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_constants(test_environment):
    declarations = """
    const current_date <- current_date();
    const current_datetime <- current_datetime();
    
    select
        current_date,
        current_datetime,
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]
    for dialect in TEST_DIALECTS:
        dialect.compile_statement(process_query(test_environment, select))


def test_unnest(test_environment):
    declarations = """
   auto int_list <- [1,2,3,4];
    auto x <- unnest(int_list);
    
    select
        x
    ;"""
    env, parsed = parse(declarations, environment=test_environment)
    assert env.concepts["int_list"].datatype == ListType(type=DataType.INTEGER)
    assert env.concepts["x"].datatype == DataType.INTEGER


def test_validate_constant_functions():
    x = Environment()
    env, _ = x.parse(
        """
            const current_date <- current_date();
            """
    )
    assert env.concepts["current_date"].purpose == Purpose.CONSTANT
    assert env.concepts["current_date"].derivation == Derivation.CONSTANT
