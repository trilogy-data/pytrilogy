
import pandas as pd
from preql import Executor, Dialects
from preql.core.models import Environment
from sqlalchemy import create_engine, text
from preql.core.models import Datasource, Concept, Function, ColumnAssignment, ConceptDeclaration, Grain
from preql.core.enums import DataType, Purpose, FunctionType
from os.path import dirname
from pathlib import PurePath
from preql import Executor
from preql.parsing.render import render_query, render_environment, Renderer
from preql.constants import logger
from logging import StreamHandler, DEBUG
from typing import Optional

logger.addHandler(StreamHandler())
logger.setLevel(DEBUG)
def setup_engine()->Executor:
    engine = create_engine(r"duckdb:///:memory:", future=True)
    csv = PurePath(dirname(__file__)) / 'train.csv'
    df = pd.read_csv(csv)
    output = Executor(engine=engine, dialect=Dialects.DUCK_DB, hooks=[])

    output.execute_raw_sql("CREATE TABLE raw_titanic AS SELECT * FROM df")
    return output


def create_passenger_dimension(exec: Executor, name: str):
    exec.execute_raw_sql(f"CREATE SEQUENCE seq_{name} START 1;")
    exec.execute_raw_sql(
        f"""create table dim_{name} as 
                         SELECT passengerid id, name, age,
                          SPLIT(name, ',')[1] passenger_last_name
                            FROM raw_data

"""
    )


def create_arbitrary_dimension(exec: Executor, key: str, name: str):
    exec.execute_raw_sql(
        f"""create table dim_{name} as 
                         with tmp as 
                         (select {key}
                         from raw_data group by 1
                         )
                         SELECT  row_number() over() as id,
                         {key} as {name}
                          FROM tmp
"""
    )


def create_fact(
    exec: Executor,
    dims: Optional[list[str]] = None,
    include: Optional[list[str]] = None,
):
    exec.execute_raw_sql(
        """create table fact_titanic as 
                         SELECT 
                         row_number() OVER () as fact_key,
                         passengerid,
                         survived,
                         fare,
                         embarked,
                         b.id class_id,
                         cabin  
                         FROM raw_data a 
                         LEFT OUTER JOIN dim_class b on a.pclass=b.class
                         """
    )


def setup_normalized_engine() -> Executor:
    engine = create_engine(r"duckdb:///:memory:", future=True)
    csv = PurePath(dirname(__file__)) / "train.csv"
    df = pd.read_csv(csv)
    _ = df
    output = Executor(engine=engine, dialect=Dialects.DUCK_DB)

    output.execute_raw_sql("CREATE TABLE raw_data AS SELECT * FROM df")
    create_passenger_dimension(output, "passenger")
    create_arbitrary_dimension(output, "pclass", "class")
    create_fact(output, ["passenger"])
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


def setup_titanic_distributed(env: Environment):
    namespace = "passenger"
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
    class_id = Concept(
        name="_class_id",
        namespace=namespace,
        purpose=Purpose.KEY,
        datatype=DataType.INTEGER,
        # keys=[id],
    )
    pclass = Concept(
        name="class",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=[class_id],
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
    last_name = Concept(
        name="last_name",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=[id],
        lineage=Function(
            operator=FunctionType.INDEX_ACCESS,
            arguments=[
                Function(
                    operator=FunctionType.SPLIT,
                    arguments=[name, ","],
                    output_datatype=DataType.ARRAY,
                    output_purpose=Purpose.PROPERTY,
                    arg_count=2,
                ),
                1,
            ],
            output_datatype=DataType.STRING,
            output_purpose=Purpose.PROPERTY,
            arg_count=2,
        ),
    )
    for x in [id, age, survived, name, pclass, fare, cabin, embarked, last_name]:
        env.add_concept(x)

    env.add_datasource(
        Datasource(
            identifier="dim_passenger",
            address="dim_passenger",
            columns=[
                ColumnAssignment(alias="id", concept=id),
                ColumnAssignment(alias="age", concept=age),
                ColumnAssignment(alias="name", concept=name),
                ColumnAssignment(alias="last_name", concept=last_name),
                # ColumnAssignment(alias="pclass", concept=pclass),
                # ColumnAssignment(alias="name", concept=name),
                # ColumnAssignment(alias="fare", concept=fare),
                # ColumnAssignment(alias="cabin", concept=cabin),
                # ColumnAssignment(alias="embarked", concept=embarked),
            ],
            grain=Grain(components=[id]),
        ),
    )

    env.add_datasource(
        Datasource(
            identifier="fact_titanic",
            address="fact_titanic",
            columns=[
                ColumnAssignment(alias="passengerid", concept=id),
                ColumnAssignment(alias="survived", concept=survived),
                ColumnAssignment(alias="class_id", concept=class_id),
                ColumnAssignment(alias="fare", concept=fare),
                ColumnAssignment(alias="cabin", concept=cabin),
                ColumnAssignment(alias="embarked", concept=embarked),
            ],
            grain=Grain(components=[id]),
        ),
    )

    env.add_datasource(
        Datasource(
            identifier="dim_class",
            address="dim_class",
            columns=[
                ColumnAssignment(alias="id", concept=class_id),
                ColumnAssignment(alias="class", concept=pclass),
                # ColumnAssignment(alias="fare", concept=fare),
                # ColumnAssignment(alias="cabin", concept=cabin),
                # ColumnAssignment(alias="embarked", concept=embarked),
            ],
            grain=Grain(components=[class_id]),
        ),
    )

    return env

# if main gating for python
if __name__ == "__main__": 
    from preql import __version__
    print(__version__)
    executor = setup_normalized_engine()
    env = Environment()
    model = setup_titanic_distributed(env)
    renderer = Renderer()
    executor.environment = env
    test = '''
property passenger.id.family <- split(passenger.name, ',')[1]; 
auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.family,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
order by
    passenger.id.count desc
limit 5;'''


    # local_opt = local_opts = get_local_optional(
    #     [env.concepts['passenger.class'],
    #     env.concepts['local.family_size']],
    #     [],
    #     env.concepts['passenger.class'],

    # )
    # queries = executor.parse_text(test)


    # rendered = executor.generate_sql(test)
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
    results = executor.execute_text(test)
    for row in results[0]:
        print(row)
#     results= executor.execute_text(
#    ''' select 
#     passenger.split_cabin;'''
#  )
    
    