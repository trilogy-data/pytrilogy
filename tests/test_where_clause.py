# from preql.compiler import compile
from preql.core.models import SelectStatement, Grain, Parenthetical
from preql.core.query_processor import process_query
from preql.dialect.base import BaseDialect
from preql.parser import parse


def test_select_where(test_environment):
    declarations = """

select
    order_id
where
    order_id in (1,2,3)
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

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
    select: SelectStatement = parsed[-1]

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
    select: SelectStatement = parsed[-1]

    BaseDialect().compile_statement(process_query(test_environment, select))


def test_select_where_joins(test_environment):
    declarations = """

select
    order_id,
    category_id
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    final = BaseDialect().compile_statement(process_query(test_environment, select))
    print(final)


def test_select_where_attribute_v2(test_environment):
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

    select: SelectStatement = parsed[-1]

    BaseDialect().compile_statement(process_query(test_environment, select))


def test_where_debug(test_environment):
    declarations = """

select
    revenue,
    order_id,
    product_id,
    category_id
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    compiled = BaseDialect().compile_statement(process_query(test_environment, select))
    print(compiled)


# TODO: determine why this is failing


def test_select_where_attribute(test_environment):
    declarations = """
property special_order_high_rev <- filter order_id where sum(revenue) by order_id > 1000;


select
    special_order_high_rev,
    total_revenue
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    query = BaseDialect().compile_statement(process_query(test_environment, select))
    print(query)


def test_parenthetical(test_environment):
    declarations = """


select
    order_id,
    total_revenue,
where 
(order_id =1 or order_id = 2) and total_revenue>30
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    left = select.where_clause.conditional.left

    assert isinstance(left, Parenthetical)

    address = set([x.address for x in left.concept_arguments])

    assert address == set(
        [
            "local.order_id",
        ]
    )

    query = BaseDialect().compile_statement(process_query(test_environment, select))
    assert "`order_id` = 1" in query


def test_like_filter(test_environment):
    declarations = """
property special_order <- filter order_id where like(category_name, 'test') = True;
property special_order_2 <- filter order_id where like(category_name, 'test') = 1;

select
    special_order
;


    """
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    assert env.concepts["special_order"].lineage.where.conditional.right is True
    assert env.concepts["special_order_2"].lineage.where.conditional.right == 1
    query = BaseDialect().compile_statement(process_query(test_environment, select))
    assert "= True" in query


def test_bare_where(test_environment):
    declarations = """
select
    category_name
where
    category_name like '%e%'
;


    """
    env, parsed = parse(declarations, environment=test_environment)
