# from trilogy.compiler import compile
from trilogy.core.models.author import Grain, Parenthetical
from trilogy.core.models.build import Factory
from trilogy.core.processing.condition_utility import is_scalar_condition
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


def test_select_hierarchical_where_order(test_environment):
    declarations = """
where
    order_id > 1
then where
    revenue > 0
then where
    total_revenue > 10
select
    total_revenue
;
"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    assert len(select.where_clauses) == 3
    assert [str(clause) for clause in select.where_clauses] == [
        "ref:local.order_id > 1",
        "ref:local.revenue > 0",
        "ref:local.total_revenue > 10",
    ]

    query = BaseDialect().compile_statement(process_query(env, select))

    assert "`revenue`.`order_id` > 1" in query
    assert "`revenue`.`revenue` > 0" in query
    assert "HAVING" in query
    assert "sum(`revenue`.`revenue`) > 10" in query


def test_select_hierarchical_where_matches_flat(test_environment):
    staged = """
where
    order_id > 1
then where
    revenue > 0
then where
    total_revenue > 10
select
    total_revenue
;
"""
    flat = """
where
    order_id > 1
    and revenue > 0
    and total_revenue > 10
select
    total_revenue
;
"""
    staged_env, staged_parsed = parse(staged, environment=test_environment)
    flat_env, flat_parsed = parse(flat, environment=test_environment)

    staged_query = BaseDialect().compile_statement(
        process_query(staged_env, staged_parsed[-1])
    )
    flat_query = BaseDialect().compile_statement(
        process_query(flat_env, flat_parsed[-1])
    )

    assert "`order_id`" in staged_query
    assert "`total_revenue`" in staged_query
    assert len(staged_parsed[-1].where_clauses) == 3
    assert len(flat_parsed[-1].where_clauses) == 1
    assert staged_query
    assert flat_query


def test_pre_and_post_select_where_are_ordered(test_environment):
    declarations = """
where
    category_id = 1
select
    category_id
where
    category_name like '%a%'
;
"""
    env, parsed = parse(declarations, environment=test_environment)
    select: SelectStatement = parsed[-1]

    assert len(select.where_clauses) == 2
    assert [str(clause) for clause in select.where_clauses] == [
        "ref:local.category_id = 1",
        "ref:local.category_name like %a%",
    ]

    BaseDialect().compile_statement(process_query(env, select))


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
    # ``like(x, 'lit')`` now parses as a ``Comparison`` so the redundant
    # ``= True`` wrapper is stripped by ``_unwrap_condition_boolean_wrapper``.
    # The surviving form ``x like 'lit'`` is identical in semantics and
    # cleaner SQL.
    assert "like 'test'" in query
    assert "= True" not in query


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
