import pandas as pd
from preql import Executor, Dialects
from preql.core.models import Environment
from sqlalchemy import create_engine
from preql.core.models import (
    Datasource,
    Concept,
    ColumnAssignment,
    Grain,
)
from preql.core.enums import DataType, Purpose
from os.path import dirname
from pathlib import PurePath
from preql.parsing.render import Renderer


def setup_engine() -> Executor:
    engine = create_engine(r"duckdb:///:memory:", future=True)
    csv = PurePath(dirname(__file__)) / "train.csv"
    df = pd.read_csv(csv)
    _ = df
    output = Executor(engine=engine, dialect=Dialects.DUCK_DB, hooks=[])

    output.execute_raw_sql("CREATE TABLE raw_titanic AS SELECT * FROM df")
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
        keys=[id],
        grain=Grain(components=[id]),
    )

    name = Concept(
        name="name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys=[id],
        grain=Grain(components=[id]),
    )

    pclass = Concept(
        name="class",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=[id],
        grain=Grain(components=[id]),
    )
    survived = Concept(
        name="survived",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.BOOL,
        keys=[id],
        grain=Grain(components=[id]),
    )
    fare = Concept(
        name="fare",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.FLOAT,
        keys=[id],
        grain=Grain(components=[id]),
    )
    embarked = Concept(
        name="embarked",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=[id],
        grain=Grain(components=[id]),
    )
    cabin = Concept(
        name="cabin",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=[id],
        grain=Grain(components=[id]),
    )
    ticket = Concept(
        name="ticket",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=[id],
        grain=Grain(components=[id]),
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


def test_demo_e2e():
    executor = setup_engine()
    env = Environment()
    setup_titanic(env)
    Renderer()
    executor.environment = env
    test = """
property passenger.id.split_cabin <- unnest(split(passenger.cabin, ' '));
persist cabin_info into dim_cabins from select passenger.id, passenger.split_cabin;
select 
    passenger.split_cabin;

"""

    executor.parse_text(test)

    executor.generate_sql(test)

    executor.execute_text(test)

    # for x, v in executor.environment.datasources.items():
    #     print(x)
    #     print(v.grain)
    #     print([str(z) for z in v.output_concepts])
    # remove raw data
    del executor.environment.datasources["raw_data"]
    executor.execute_text(
        """ select
   passenger.id,
    passenger.split_cabin;"""
    )
    # confirm we can still get results





def test_demo_aggregates():
    executor = setup_engine()
    env = Environment()
    setup_titanic(env)
    Renderer()
    executor.environment = env
    test = """
auto survivor <- filter passenger.id where passenger.survived = 1;
select passenger.class, 
(count(survivor) by passenger.class/count(passenger.id) by passenger.class)*100->survival_rate;
"""

    executor.parse_text(test)

    executor.generate_sql(test)

    results = executor.execute_text(test)

    for row in results[0]:
        assert row.survival_rate <100
