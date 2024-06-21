from preql import Executor, Dialects

# from preql.core.models import Environment
from sqlalchemy import create_engine
from preql.core.models import (
    DataType,
    Datasource,
    Concept,
    ColumnAssignment,
    Environment,
)
from preql.core.enums import Purpose

from preql.constants import logger

from logging import StreamHandler
from preql.core.query_processor import generate_graph
from preql.core.processing.nodes import MergeNode
from preql.core.processing.concept_strategies_v3 import search_concepts
from preql.core.processing.node_generators import (
    gen_filter_node,
)


logger.addHandler(StreamHandler())


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
        keys=(id,),
    )

    name = Concept(
        name="name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys=(id,),
    )

    pclass = Concept(
        name="passenger_class",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=(id,),
    )
    survived = Concept(
        name="survived",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.BOOL,
        keys=(id,),
    )
    fare = Concept(
        name="fare",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.FLOAT,
        keys=(id,),
    )
    for x in [id, age, survived, name, pclass, fare]:
        env.add_concept(x)

    env.add_datasource(
        Datasource(
            identifier="raw_data",
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
    from preql.hooks.query_debugger import DebuggingHook
    from preql.core.processing.node_generators.common import concept_to_relevant_joins

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
limit 5;"""
    results = executor.parse_text(test)
    family = env.concepts["passenger.family"]
    id = env.concepts["passenger.id"]
    g = generate_graph(env)
    filtered_node = gen_filter_node(
        env.concepts["surviving_passenger"],
        [family],
        environment=env,
        g=g,
        depth=0,
        source_concepts=search_concepts,
    )
    assert len(filtered_node.partial_concepts) == 2
    assert set([c.address for c in filtered_node.partial_concepts]) == set(
        [c.address for c in [family, id]]
    )

    # now resolve the node
    # and make sure it also has partial tags
    resolved = filtered_node.resolve()

    assert len(resolved.partial_concepts) == 2
    assert set([c.address for c in resolved.partial_concepts]) == set(
        [c.address for c in [family, id]]
    )

    # check at the source level
    sourced = search_concepts(
        [family, env.concepts["surviving_passenger"]], environment=env, g=g, depth=0
    )
    assert isinstance(sourced, MergeNode)
    assert len(sourced.parents) == 2
    # filter
    mnode = [node for node in sourced.parents if node.partial_concepts][0]
    # selectnode = [node for node in sourced.parents if isinstance(node, SelectNode)][0]
    resolved = sourced.resolve().source_map["passenger.family"]

    join_concepts = [
        env.concepts[v]
        for v in ["passenger.id", "passenger.family", "passenger.survived"]
    ]

    assert (
        len(concept_to_relevant_joins(join_concepts)) == 1
    ), "should only have one join concept"

    assert len(mnode.partial_concepts) == 2
    assert set([c.address for c in mnode.partial_concepts]) == set(
        [c.address for c in [family, id]]
    )

    filter_node = sourced.parents[0]
    assert isinstance(filter_node, MergeNode), str(type(filter_node)) + str(filter_node)

    assert len(filter_node.partial_concepts) == 2
    assert set([c.address for c in filter_node.partial_concepts]) == set(
        [c.address for c in [family, id]]
    )

    # and now a full E2E
    query = results[-1]
    x = query.ctes[-1].parent_ctes[0]
    # parent = x.parent_ctes[0]
    assert x.get_alias(family) != x.get_alias(env.concepts["surviving_size"])
    assert "local_surviving_passenger" not in x.source.source_map["passenger.family"]
    non_filtered_found = False
    for source in x.source.source_map["passenger.family"]:
        # ensure we don't get this from a filtered source
        non_filtered_found = True
        if "filtered" not in source.identifier:
            non_filtered_found = True

    assert non_filtered_found, x.source.source_map["passenger.family"]
