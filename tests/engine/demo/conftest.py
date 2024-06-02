import pandas as pd
from preql import Executor, Dialects
from preql.core.models import Environment
from sqlalchemy import create_engine
from preql.core.models import (
    Datasource,
    Concept,
    ColumnAssignment,
    Grain,
    DataType,
    Function,
    Metadata,
)
from preql.core.enums import Purpose, FunctionType
from os.path import dirname
from pathlib import PurePath
from preql.hooks.query_debugger import DebuggingHook
from logging import INFO
from typing import Optional
from preql.core.functions import function_args_to_output_purpose, arg_to_datatype

from pytest import fixture


def create_passenger_dimension(exec: Executor, name: str):
    exec.execute_raw_sql(f"CREATE SEQUENCE seq_{name} START 1;")
    exec.execute_raw_sql(
        f"""create table dim_{name} as 
                         SELECT passengerid id, name, age,
                          SPLIT(name, ',')[1] last_name
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
    output = Executor(
        engine=engine, dialect=Dialects.DUCK_DB, hooks=[DebuggingHook(level=INFO)]
    )

    output.execute_raw_sql("CREATE TABLE raw_data AS SELECT * FROM df")
    create_passenger_dimension(output, "passenger")
    create_arbitrary_dimension(output, "pclass", "class")
    create_fact(output, ["passenger"])
    return output


def create_function_derived_concept(
    name: str,
    namespace: str,
    operator: FunctionType,
    arguments: list[Concept],
    output_type: Optional[DataType] = None,
    output_purpose: Optional[Purpose] = None,
    metadata: Optional[Metadata] = None,
) -> Concept:
    purpose = (
        function_args_to_output_purpose(arguments)
        if output_purpose is None
        else output_purpose
    )
    output_type = (
        arg_to_datatype(arguments[0]).data_type if output_type is None else output_type
    )
    return Concept(
        name=name,
        namespace=namespace,
        datatype=output_type,
        purpose=purpose,
        lineage=Function(
            operator=operator,
            arguments=arguments,
            output_datatype=output_type,
            output_purpose=purpose,
            arg_count=len(arguments),
        ),
        metadata=metadata,
    )


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
    survived_count = create_function_derived_concept(
        "survived_count",
        namespace,
        FunctionType.SUM,
        [survived],
        output_purpose=Purpose.METRIC,
    )

    for x in [
        id,
        age,
        survived,
        name,
        pclass,
        fare,
        cabin,
        embarked,
        survived_count,
        last_name,
    ]:
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


@fixture
def normalized_engine():
    executor = setup_normalized_engine()
    yield executor


@fixture
def test_env():
    env = Environment()
    setup_titanic_distributed(env)
    yield env
