
import pandas as pd
from preql import Executor, Dialects
from preql.core.models import Environment
from sqlalchemy import create_engine, text
from preql.core.models import Datasource, Concept, ColumnAssignment, ConceptDeclaration, Grain
from preql.core.enums import DataType, Purpose
from os.path import dirname
from pathlib import PurePath
from preql import Executor
from preql.parsing.render import render_query, render_environment, Renderer
from preql.constants import logger
from preql.hooks.query_debugger import DebuggingHook
from preql.core.processing.node_generators import (
    gen_filter_node,
    gen_window_node,
    gen_group_node,
    gen_basic_node,
    gen_select_node,
    gen_static_select_node,
)
from preql.core.processing.concept_strategies_v2 import generate_graph
from logging import StreamHandler

logger.addHandler(StreamHandler())
def setup_engine()->Executor:
    engine = create_engine(r"duckdb:///:memory:", future=True)
    csv = PurePath(dirname(__file__)) / 'train.csv'
    df = pd.read_csv(csv)
    output = Executor(engine=engine, dialect=Dialects.DUCK_DB, hooks=[])

    output.execute_raw_sql("CREATE TABLE raw_titanic AS SELECT * FROM df")
    return output


def setup_titanic(env:Environment):
    namespace = 'passenger'
    id = Concept(
        name="id", namespace=namespace, datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    age = Concept(
        name="age",
        namespace=namespace,
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        keys=[id],
    )

    name = Concept(
        name="name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys=[id],
    )

    pclass = Concept(
        name="passenger_class",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=[id],
    )
    survived = Concept(
        name="survived",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.BOOL,
        keys=[id],
    )
    fare = Concept(
        name="fare",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.FLOAT,
        keys=[id],
    )
    embarked = Concept(
        name="embarked",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=[id],
    )
    cabin = Concept(
        name="cabin",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=[id],
    )
    for x in [id, age, survived, name, pclass, fare, cabin, embarked]:
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
                ColumnAssignment(alias="cabin", concept=cabin),
                ColumnAssignment(alias="embarked", concept=embarked),
            ],
            grain=Grain(components=[id]),
        ),
    )
    return env


# if main gating for python
if __name__ == "__main__": 
    from preql import __version__
    print(__version__)
    executor = setup_engine()
    env = Environment()
    model = setup_titanic(env)
    renderer = Renderer()
    for c, conc in env.concepts.items():
        print(c)

        print(renderer.to_string(ConceptDeclaration(concept=conc)))

    executor.environment = env
    test = '''property passenger.id.family <- split(passenger.name, ',')[1];
auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.family,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
where
    passenger.id.count>4
order by
    passenger.id.count desc
limit 5;'''
    node = gen_select_node(
        concept =env.concepts['passenger.name'],
        local_optional = [env.concepts['passenger.age'].with_grain(Grain())],
        environment=env, 
                    g = generate_graph(env),
                    depth = 1,
                    source_concepts= lambda: 1,)
    print(node.resolve())

    queries = executor.parse_text(test)
    candidate = queries[-1]
    print(candidate.grain)
    for z in candidate.output_columns:
        print(z)
    # alias = candidate.base.get_alias(executor.environment.concepts['passenger.family'])
    # family_source = [c for c in  candidate.ctes if c.name == candidate.base.source_map['passenger.family']][0]

    # print(family_source.source.source_map.keys())

    results= executor. execute_text(test)
    for r in results[0]:
        print(r)
    print('-------------')