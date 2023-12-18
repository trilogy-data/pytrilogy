
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
    test = '''
property passenger.id.non_null_cabin <- filter passenger.cabin where passenger.cabin is not null;
select 
    unnest(split(passenger.non_null_cabin, ' '))-> split_cabins, 
    passenger.cabin,
    passenger.name,
order by split_cabins asc
limit 120
;

'''



    # local_opt = local_opts = get_local_optional(
    #     [env.concepts['passenger.class'],
    #     env.concepts['local.family_size']],
    #     [],
    #     env.concepts['passenger.class'],

    # )
    queries = executor.parse_text(test)
    rendered = executor.generate_sql(test)
    # print(family_source.source.source_map.keys())
#     results = executor.execute_raw_sql("""WITH tmp as (SELECT
#     SPLIT(local_raw_data."cabin", ' ') as "passenger_cabin",
#     local_raw_data."name" as "passenger_name"
# FROM
#     raw_titanic as local_raw_data
#                                        where cabin is not null)
#     select
#         passenger_name,
#         z.passenger_cabin
#     from tmp
#     CROSS JOIN unnest(passenger_cabin) as z 
#     order by z.passenger_cabin desc
#                              """)
    print(rendered[0])
    results= executor.execute_raw_sql(
        rendered[0] )
    for r in results:
        print(r)
    print('-------------')
    