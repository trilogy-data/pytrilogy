# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.core.query_processor import process_query
from preql.dialect.base import BaseDialect
from preql.parser import parse

import pytest


def test_select_where(test_environment):
    declarations = """

select
    order_id
where
    order_id in (1,2,3)
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    assert select.grain == Grain(components=[env.concepts["order_id"]])

    BaseDialect().compile_statement(process_query(test_environment, select))


def test_select_where_or(test_environment):
    declarations = """

select
    order_id
where
    order_id =1 or order_id =2 or order_id = 3
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    assert select.grain == Grain(components=[env.concepts["order_id"]])

    BaseDialect().compile_statement(process_query(test_environment, select))


def test_select_where_agg(test_environment):
    declarations = """
property my_favorite_order_revenue <- filter revenue where order_id in (1,2,3);

metric my_favorite_order_total_revenue <- sum(my_favorite_order_revenue);
select
    my_favorite_order_total_revenue
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    BaseDialect().compile_statement(process_query(test_environment, select))


def test_select_where_joins(test_environment, logger):
    declarations = """

select
    order_id,
    category_id
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    final = BaseDialect().compile_statement(process_query(test_environment, select))
    print(final)


def test_select_where_attribute_v2(test_environment, logger):
    declarations = """
key special_category <- filter category_id where like(category_name, '%special%') is True;

select
    order_id,
    special_category
;


    """
    env, parsed = parse(declarations, environment=test_environment)

    spec_category = env.concepts["special_category"]
    category_name = env.concepts["category_name"]
    inputs = [x.address for x in spec_category.lineage.where.input]
    assert len(inputs) == 1
    assert category_name.address in inputs

    select: Select = parsed[-1]

    cmd = BaseDialect().compile_statement(process_query(test_environment, select))


def test_where_debug(test_environment, logger):
    declarations = """

select
    revenue,
    order_id,
    product_id,
    category_id
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    compiled = BaseDialect().compile_statement(process_query(test_environment, select))
    print(compiled)


# TODO: determine why this is failing


def test_select_where_attribute(test_environment, logger):
    declarations = """
property special_order_high_rev <- filter order_id where total_revenue > 1000;


select
    special_order_high_rev,
    total_revenue
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: Select = parsed[-1]

    query = BaseDialect().compile_statement(process_query(test_environment, select))
    print(query)
