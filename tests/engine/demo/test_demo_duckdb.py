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
)
from preql.core.enums import Purpose, FunctionType
from os.path import dirname
from pathlib import PurePath
from preql.hooks.query_debugger import DebuggingHook
from logging import INFO


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
    executor.environment = env
    test = """
key survivor <- filter passenger.id where passenger.survived = 1;

auto survivors <- count(survivor) by passenger.class;
auto total <- count(passenger.id) by passenger.class;
auto ratio <- survivors/total;
auto survival_rate <- ratio*100;
select 
    passenger.class, 
    survival_rate;
"""

    executor.parse_text(test)
    ratio = env.concepts["ratio"]
    assert ratio.purpose == Purpose.METRIC
    assert env.concepts["ratio"].grain == Grain(
        components=[env.concepts["passenger.class"]]
    )

    results = executor.execute_text(test)

    for row in results[0]:
        assert row.survival_rate < 100


def test_demo_filter():
    executor = setup_engine(debug_flag=False)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """
    auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
    select     passenger.last_name,    passenger.id.count,   
      count(surviving_passenger) -> surviving_size
      where    
      passenger.id.count=surviving_size
    order by passenger.id.count desc, passenger.last_name asc
    limit 5;"""

    results = executor.execute_text(test)[-1].fetchall()

    assert results[0].passenger_last_name == "Baclini"

    test = """
    auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
    select     passenger.last_name,    passenger.id.count,   
      count(surviving_passenger) -> surviving_size
    order by surviving_size desc, passenger.id.count desc
    limit 5;"""

    results = executor.execute_text(test)[-1].fetchall()

    assert results[0].passenger_last_name == "Carter"


def test_demo_const():
    executor = setup_engine(debug_flag=False)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """
    const right_now <- current_datetime(); select right_now;"""

    executor.execute_text(test)[-1].fetchall()


def test_demo_rowset():
    executor = setup_engine(debug_flag=False)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """rowset survivors<- select 
    passenger.last_name, 
    passenger.name,
    passenger.id, 
    passenger.survived,
    passenger.age,  
where 
    passenger.survived =1; 

# now we can reference our rowset like any other concept
select 
    --survivors.passenger.id,
    survivors.passenger.name,
    survivors.passenger.last_name,
    survivors.passenger.age,
    --row_number survivors.passenger.id over survivors.passenger.name order by survivors.passenger.age desc -> eldest
where 
    eldest = 1
order by survivors.passenger.name desc
limit 5;"""
    # raw = executor.generate_sql(test)
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 5
    assert len(results[0]) == 3
