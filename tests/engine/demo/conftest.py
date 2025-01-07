from logging import INFO
from os.path import dirname
from pathlib import PurePath
from typing import Optional

import pandas as pd
from pytest import fixture
from sqlalchemy import create_engine

from trilogy import Dialects, Executor
from trilogy.core.enums import FunctionType, Modifier, Purpose
from trilogy.core.functions import arg_to_datatype, function_args_to_output_purpose
from trilogy.core.models_core import (
    DataType,
)
from trilogy.core.models_author import Concept, Function, Grain, Metadata
from trilogy.core.models_datasource import ColumnAssignment, Datasource
from trilogy.core.models_environment import Environment
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parsing.common import function_to_concept


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
    output = Executor(
        engine=engine, dialect=Dialects.DUCK_DB, hooks=[DebuggingHook(level=INFO)]
    )
    df = pd.read_csv(csv)
    output.execute_raw_sql("register(:name, :df)", {"name": "df", "df": df})
    output.execute_raw_sql("CREATE TABLE raw_data AS SELECT * FROM df")
    create_passenger_dimension(output, "passenger")
    create_arbitrary_dimension(output, "pclass", "class")
    create_fact(output, ["passenger"])
    df2 = pd.read_csv(PurePath(dirname(__file__)) / "richest.csv")
    _ = df2
    output.execute_raw_sql("register(:name, :df)", {"name": "df2", "df": df2})
    output.execute_raw_sql("CREATE TABLE rich_info AS SELECT * FROM df2")
    return output


def setup_engine(debug_flag: bool = True) -> Executor:
    engine = create_engine(r"duckdb:///:memory:", future=True)
    csv = PurePath(dirname(__file__)) / "train.csv"
    df = pd.read_csv(csv)
    _ = df
    output = Executor(
        engine=engine,
        dialect=Dialects.DUCK_DB,
        hooks=(
            [
                DebuggingHook(
                    level=INFO,
                    process_other=False,
                    process_datasources=False,
                    process_ctes=False,
                )
            ]
            if debug_flag
            else []
        ),
    )
    output.execute_raw_sql("register(:name, :df)", {"name": "df", "df": df})
    output.execute_raw_sql("CREATE TABLE raw_titanic AS SELECT * FROM df")
    df2 = pd.read_csv(PurePath(dirname(__file__)) / "richest.csv")
    _ = df2
    output.execute_raw_sql("register(:name, :df)", {"name": "df2", "df": df2})
    output.execute_raw_sql("CREATE TABLE rich_info AS SELECT * FROM df2")
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


def setup_richest_environment(env: Environment):
    namespace = None
    name = Concept(
        name="full_name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
    )
    money = Concept(
        name="net_worth_1918_dollars",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys=(name.address,),
    )
    env.add_concept(name)
    split_name = function_to_concept(
        Function(
            operator=FunctionType.SPLIT,
            arguments=[name, " "],
            output_datatype=DataType.ARRAY,
            output_purpose=Purpose.PROPERTY,
            arg_count=2,
        ),
        name="split_name",
        namespace=namespace,
        environment=env,
        # keys = (name,)
    )
    last_name = Concept(
        name="last_name",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=(name.address,),
        lineage=Function(
            operator=FunctionType.INDEX_ACCESS,
            arguments=[
                split_name,
                -1,
            ],
            output_datatype=DataType.STRING,
            output_purpose=Purpose.PROPERTY,
            arg_count=2,
        ),
    )
    for x in [name, money, last_name, split_name]:
        env.add_concept(x)

    env.add_datasource(
        Datasource(
            name="rich_info",
            address="rich_info",
            columns=[
                ColumnAssignment(alias="Name", concept=name),
                ColumnAssignment(alias="Net Worth 1918 Dollars", concept=money),
            ],
        )
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
        keys=(id.address,),
        modifiers=[Modifier.NULLABLE],
    )

    name = Concept(
        name="name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys=(id.address,),
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
        keys=(class_id.address,),
    )
    survived = Concept(
        name="survived",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=(id.address,),
    )
    fare = Concept(
        name="fare",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.FLOAT,
        keys=(id.address,),
    )
    embarked = Concept(
        name="embarked",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=(id.address,),
    )
    cabin = Concept(
        name="cabin",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=(id.address,),
    )
    env.add_concept(name)
    env.add_concept(id)
    split_name = function_to_concept(
        Function(
            operator=FunctionType.SPLIT,
            arguments=[name, ","],
            output_datatype=DataType.ARRAY,
            output_purpose=Purpose.PROPERTY,
            arg_count=2,
        ),
        name="split_name",
        namespace=namespace,
        environment=env,
        # keys = (id,)
    )
    assert split_name.keys == {
        id.address,
    }
    last_name = Concept(
        name="last_name",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=(id.address,),
        lineage=Function(
            operator=FunctionType.INDEX_ACCESS,
            arguments=[
                split_name,
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
        class_id,
    ]:
        env.add_concept(x)

    env.add_datasource(
        Datasource(
            name="dim_passenger",
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
            grain=Grain(
                components={
                    id.address,
                }
            ),
        ),
    )

    env.add_datasource(
        Datasource(
            name="fact_titanic",
            address="fact_titanic",
            columns=[
                ColumnAssignment(alias="passengerid", concept=id),
                ColumnAssignment(alias="survived", concept=survived),
                ColumnAssignment(alias="class_id", concept=class_id),
                ColumnAssignment(alias="fare", concept=fare),
                ColumnAssignment(alias="cabin", concept=cabin),
                ColumnAssignment(alias="embarked", concept=embarked),
            ],
            grain=Grain(
                components={
                    id.address,
                }
            ),
        ),
    )

    env.add_datasource(
        Datasource(
            name="dim_class",
            address="dim_class",
            columns=[
                ColumnAssignment(alias="id", concept=class_id),
                ColumnAssignment(alias="class", concept=pclass),
                # ColumnAssignment(alias="fare", concept=fare),
                # ColumnAssignment(alias="cabin", concept=cabin),
                # ColumnAssignment(alias="embarked", concept=embarked),
            ],
            grain=Grain(components={class_id.address}),
        ),
    )
    return env


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
        keys=(id.address,),
        grain=Grain(components=[id]),
        modifiers=[Modifier.NULLABLE],
    )

    name = Concept(
        name="name",
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        keys=(id.address,),
        grain=Grain(components=[id]),
    )

    pclass = Concept(
        name="class",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=(id.address,),
        grain=Grain(components=[id]),
    )
    survived = Concept(
        name="survived",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=(id.address,),
        grain=Grain(components=[id]),
    )
    fare = Concept(
        name="fare",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.FLOAT,
        keys=(id.address,),
        grain=Grain(components=[id]),
    )
    embarked = Concept(
        name="embarked",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.INTEGER,
        keys=(id.address,),
        grain=Grain(components=[id]),
    )
    cabin = Concept(
        name="cabin",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=(id.address,),
        grain=Grain(components=[id]),
    )
    ticket = Concept(
        name="ticket",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=(id.address,),
        grain=Grain(components=[id]),
    )

    last_name = Concept(
        name="last_name",
        namespace=namespace,
        purpose=Purpose.PROPERTY,
        datatype=DataType.STRING,
        keys=(id.address,),
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
    for x in [
        id,
        age,
        survived,
        name,
        last_name,
        pclass,
        fare,
        cabin,
        embarked,
        ticket,
    ]:
        env.add_concept(x)
    assert name in last_name.sources
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
                ColumnAssignment(alias="cabin", concept=cabin),
                ColumnAssignment(alias="embarked", concept=embarked),
                ColumnAssignment(alias="ticket", concept=ticket),
            ],
            grain=Grain(components={id.address}),
        ),
    )
    return env


@fixture
def normalized_engine():
    executor = setup_normalized_engine()
    yield executor


@fixture
def engine():
    executor = setup_engine()
    yield executor


@fixture
def base_test_env():
    env = Environment()
    env = setup_titanic(env)
    rich_env = Environment()
    setup_richest_environment(rich_env)
    env.add_import("rich_info", rich_env)
    yield env


@fixture
def test_env():
    env = Environment()
    env = setup_titanic_distributed(env)
    rich_env = Environment()
    setup_richest_environment(rich_env)
    env.add_import("rich_info", rich_env)
    yield env
