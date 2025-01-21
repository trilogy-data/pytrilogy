from sqlalchemy import create_engine

from trilogy import Dialects, Executor
from trilogy.core.enums import Purpose
from trilogy.core.models.author import Concept
from trilogy.core.models.core import (
    DataType,
)
from trilogy.core.models.datasource import ColumnAssignment, Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.processing.concept_strategies_v3 import History, search_concepts
from trilogy.core.processing.node_generators import (
    gen_filter_node,
)
from trilogy.core.processing.nodes import MergeNode
from trilogy.core.query_processor import generate_graph
from trilogy.hooks.query_debugger import DebuggingHook


def setup_engine() -> Executor:
    engine = create_engine(r"duckdb:///:memory:", future=True)
    output = Executor(engine=engine, dialect=Dialects.DUCK_DB)
    return output


def setup_titanic(env: Environment):
    namespace = "passenger"
    id = Concept(
        name="id", namespace=namespace, datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    age = Concept(
        name="age",
        namespace=namespace,
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        keys={
            id.address,
        },
    )

    name = Concept(
        name="name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys={
            id.address,
        },
    )

    pclass = Concept(
        name="passenger_class",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys={
            id.address,
        },
    )
    survived = Concept(
        name="survived",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys={
            id.address,
        },
    )
    fare = Concept(
        name="fare",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.FLOAT,
        keys={
            id.address,
        },
    )
    for x in [id, age, survived, name, pclass, fare]:
        env.add_concept(x)

    env.add_datasource(
        Datasource(
            name="raw_data",
            address="raw_titanic",
            columns=[
                ColumnAssignment(alias="passengerid", concept=id),
                ColumnAssignment(alias="age", concept=age),
                ColumnAssignment(alias="survived", concept=survived),
                ColumnAssignment(alias="pclass", concept=pclass),
                ColumnAssignment(alias="name", concept=name),
                ColumnAssignment(alias="fare", concept=fare),
            ],
        ),
    )
    return env


def test_partial_assignment():
    """
    Test that a derived concept is pulled from the
    CTE that has the full concept, not a partially filtered copy"""

    executor = setup_engine()
    env = Environment()
    history = History(base_environment=env)
    setup_titanic(env)
    executor.environment = env
    executor.hooks = [DebuggingHook()]
    test = """property passenger.id.family <- split(passenger.name, ',')[1]; 
    auto surviving_passenger<- filter passenger.id where passenger.survived =1;"""
    _ = executor.parse_text(test)
    env = env.materialize_for_select()
    family = env.concepts["passenger.family"]
    # id = env.concepts["passenger.id"]
    # survived = env.concepts["passenger.survived"]
    g = generate_graph(env)
    filtered_node = gen_filter_node(
        env.concepts["surviving_passenger"],
        [family],
        history=history,
        environment=env,
        g=g,
        depth=0,
        source_concepts=search_concepts,
    )
    assert len(filtered_node.partial_concepts) == 0

    # now resolve the node
    # and make sure it also has partial tags
    resolved = filtered_node.resolve()

    assert len(resolved.partial_concepts) == 0

    # check at the source level
    sourced = search_concepts(
        [family, env.concepts["surviving_passenger"]],
        history=history,
        environment=env,
        g=g,
        depth=0,
    )
    assert isinstance(sourced, MergeNode)
    assert len(sourced.parents) == 2


def test_filter_query():
    executor = setup_engine()
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    executor.hooks = [DebuggingHook()]
    test = """property passenger.id.family <- split(passenger.name, ',')[1]; 
    auto surviving_passenger<- filter passenger.id where passenger.survived =1;
    select 
    passenger.family,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
order by
    passenger.id.count desc
limit 5;
    """
    results = executor.parse_text(test)
    query = results[-1]
    x = query.ctes[-1].parent_ctes[0]
    # parent = x.parent_ctes[0]
    assert "local_surviving_passenger" not in x.source.source_map["passenger.family"]
    non_filtered_found = False
    for source in x.source.source_map["passenger.family"]:
        # ensure we don't get this from a filtered source
        non_filtered_found = True
        if "filtered" not in source.identifier:
            non_filtered_found = True

    assert non_filtered_found, x.source.source_map["passenger.family"]
