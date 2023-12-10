
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
from preql.core.processing.concept_strategies_v2 import generate_graph,  get_local_optional
from logging import StreamHandler, DEBUG

logger.addHandler(StreamHandler())
logger.setLevel(DEBUG)
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
        grain = Grain(components=[id]),
    )

    name = Concept(
        name="name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys=[id],
             grain = Grain(components=[id]),
    )

    pclass = Concept(
        name="class",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=[id],
             grain = Grain(components=[id]),
    )
    survived = Concept(
        name="survived",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.BOOL,
        keys=[id],
             grain = Grain(components=[id]),
    )
    fare = Concept(
        name="fare",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.FLOAT,
        keys=[id],
             grain = Grain(components=[id]),
    )
    embarked = Concept(
        name="embarked",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=[id],
             grain = Grain(components=[id]),
    )
    cabin = Concept(
        name="cabin",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=[id],
             grain = Grain(components=[id]),
    )
    ticket = Concept(
        name="ticket",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=[id],
             grain = Grain(components=[id]),
    )
    for x in [id, age, survived, name, pclass, fare, cabin, embarked, ticket]:
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
                                ColumnAssignment(alias="ticket", concept=ticket),
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
    executor.environment = env
    test = '''property passenger.id.family <- split(passenger.name, ',')[1];

auto survivor <- filter passenger.id where passenger.survived = 1;
auto family_survival_rate <- count(survivor)/count(passenger.id) by passenger.family;
auto family_size <- count(passenger.id) by passenger.family;
select 
    passenger.class,
    avg(family_survival_rate) -> avg_class_family_survival_rate,
    avg(family_size) -> avg_class_family_size
order by 
    passenger.class asc
;

'''
    node = gen_select_node(
        concept =env.concepts['passenger.name'],
        local_optional = [env.concepts['passenger.age'].with_grain(Grain())],
        environment=env, 
                    g = generate_graph(env),
                    depth = 1,
                    source_concepts= lambda: 1,)
    # print(node.resolve())

    # local_opt = local_opts = get_local_optional(
    #     [env.concepts['passenger.class'],
    #     env.concepts['local.family_size']],
    #     [],
    #     env.concepts['passenger.class'],

    # )
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