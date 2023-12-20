
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
property passenger.id.split_cabin <- unnest(split(passenger.cabin, ' '));
persist cabin_info into dim_cabins from select passenger.id, passenger.split_cabin;
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
    print(rendered[-1])
    executor.execute_text(test)
#     results= executor.execute_text(
#    ''' select 
#     passenger.split_cabin;'''
#  )
    
    for x, v in executor.environment.datasources.items():
        print(x)
        print(v.grain)
        print([str(z) for z in v.output_concepts])
    # del executor.environment.datasources['raw_data']
    results= executor.execute_text(
   ''' select
   passenger.id,
    passenger.split_cabin;'''
 )
    for r in results:
        for z in r.fetchall()[:5]:
            print(z)
    print('-------------')
    