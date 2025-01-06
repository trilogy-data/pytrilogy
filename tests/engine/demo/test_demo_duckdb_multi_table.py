from trilogy.core.enums import Purpose
from trilogy.core.models_execute import (
    CTE,
)
from trilogy.core.processing.nodes import SelectNode
from trilogy.core.processing.nodes.base_node import StrategyNode


def fingerprint(node: StrategyNode) -> str:
    base = node.__class__.__name__ + ",".join(
        [fingerprint(node) for node in node.parents]
    )
    if isinstance(node, SelectNode):
        base += node.datasource.name
    base += str(node.conditions)
    base += str(node.force_group)
    return base


def dedupe_nodes(nodes):
    seen = set()
    output = []
    for node in nodes:
        if fingerprint(node) not in seen:
            output.append(node)
            seen.add(fingerprint(node))
    return output


def _get_parents(node: StrategyNode):
    output = [node]
    for parent in node.parents:
        output = _get_parents(parent) + output
    return dedupe_nodes(output)


def get_parents(node: StrategyNode):
    return [node.__class__ for node in _get_parents(node)]


def test_demo_filter(normalized_engine, test_env):
    executor = normalized_engine
    env = test_env
    assert "passenger.id.count" not in env.materialized_concepts
    executor.environment = env

    test = """
    auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.last_name,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
order by
    passenger.id.count desc
limit 5;"""

    executor.parse_text(test)[-1]

    # ensure that last name is only fetched from the dimension
    last_name = env.concepts["passenger.last_name"]

    def recurse_cte(cte: CTE):
        if last_name in cte.output_columns:
            assert last_name.address in cte.source_map
        for parent in cte.parent_ctes:
            recurse_cte(parent)


def test_rowset_shape(normalized_engine, test_env):
    executor = normalized_engine
    env = test_env
    assert "passenger.id.count" not in env.materialized_concepts
    executor.environment = env

    test = """
    rowset survivors<- select 
    passenger.last_name, 
    passenger.name,
    passenger.id, 
    passenger.survived,
    passenger.age,  
where 
    passenger.survived =1; 

auto eldest <- row_number survivors.passenger.id over survivors.passenger.name order by survivors.passenger.age desc;
# now we can reference our rowset like any other concept
select 
    --survivors.passenger.id,
    survivors.passenger.name,
    survivors.passenger.last_name,
    survivors.passenger.age,
    -- eldest
where
    eldest = 1
order by survivors.passenger.name desc
limit 5;"""

    sql = executor.parse_text(test)[-1]

    assert env.concepts["local.eldest"].purpose == Purpose.PROPERTY

    # actual = executor.generate_sql(sql)
    # assert actual == ''
    # g = generate_graph(env)
    # validate_shape(
    #     sql.output_columns,
    #     env,
    #     g,
    #     levels=[
    #         SelectNode,  # select store
    #         SelectNode,  # select year
    #         MergeNode,  # basic node
    #         WindowNode,  # add window
    #         MergeNode,  # final node
    #         MergeNode,  # final node
    #         GroupNode,  # final node
    #     ],
    # )
    # STRING_SPLIT( dim_passenger."name" , ',' )[1
    rendered = executor.generate_sql(sql)[-1]
    assert """STRING_SPLIT( dim_passenger."name" , ',' )[1""" not in rendered
    sql = executor.execute_text(test)[-1].fetchall()
    assert len(sql) == 5


def test_age_class_query_resolution(normalized_engine, test_env):
    executor = normalized_engine
    env = test_env
    executor.environment = env
    test = """
SELECT
passenger.class,
passenger.id.count,
;"""

    _ = executor.parse_text(test)[-1]
