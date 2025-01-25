# from trilogy.compiler import compile
from trilogy.core.models.author import Grain, Parenthetical
from trilogy.core.models.build import Factory
from trilogy.core.processing.utility import is_scalar_condition
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.base import BaseDialect
from trilogy.parser import parse


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
    from trilogy.hooks.query_debugger import DebuggingHook

    declarations = """
property my_favorite_order_revenue <- filter revenue where order_id in (1,2,3);

metric my_favorite_order_total_revenue <- sum(my_favorite_order_revenue);
select
    my_favorite_order_total_revenue
;


    """
    env, parsed = parse(
        declarations,
        environment=test_environment,
    )
    select: SelectStatement = parsed[-1]

    BaseDialect().compile_statement(
        process_query(test_environment, select, hooks=[DebuggingHook()])
    )


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
auto special_category <- filter category_id where like(category_name, '%special%') is True;

select
    order_id,
    special_category
;


    """
    env, parsed = parse(declarations, environment=test_environment)

    spec_category = env.concepts["special_category"]
    category_name = env.concepts["category_name"]
    inputs = [x.address for x in spec_category.lineage.where.concept_arguments]
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
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    declarations = """

where 
(order_id = 1 or order_id = 2) and total_revenue>30
select
    order_id,
    total_revenue,
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
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    declarations = """
property special_order <- filter order_id where like(category_name, 'test') = True;
property special_order_2 <- filter order_id where like(category_name, 'test') is True;

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


def test_where_scalar(test_environment):
    declarations = """
select
    category_name
where
    count(order_id) > 1
;
"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]
    factory = Factory(environment=test_environment)
    assert (
        is_scalar_condition(
            factory.build(select.as_lineage(test_environment)).where_clause.conditional
        )
        is False
    )
    _ = BaseDialect().compile_statement(process_query(test_environment, select))


def test_case_where(test_environment):
    from trilogy.hooks.query_debugger import DebuggingHook

    declarations = """property order_id_even_name <- CASE
    when order_id %2 = 0 then 'even'
    else 'odd'
    END;

const test <- 1;
    
auto order_even_class_filter <- filter category_id where order_id_even_name = 'even' and 1= test; 

select
    category_id,
    category_name
where
    category_name like '%abc%' and category_id not in order_even_class_filter
    and category_id = test
;"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    query = BaseDialect().compile_statement(
        process_query(test_environment, select, hooks=[DebuggingHook()])
    )

    # check to make sure our subselect is well-formed
    assert "`category_id` not in (select" in query, query
