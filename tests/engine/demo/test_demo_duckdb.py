import pandas as pd
from trilogy import Executor, Dialects
from trilogy.core.models import Environment
from sqlalchemy import create_engine
from trilogy.core.models import (
    Datasource,
    Concept,
    ColumnAssignment,
    Grain,
    DataType,
    Function,
    LooseConceptList,
    SelectGrain,
)
from trilogy.core.enums import Purpose, FunctionType
from os.path import dirname
from pathlib import PurePath
from trilogy.hooks.query_debugger import DebuggingHook
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
    df2 = pd.read_csv(PurePath(dirname(__file__)) / "richest.csv")
    _ = df2
    output.execute_raw_sql("CREATE TABLE rich_info AS SELECT * FROM df2")
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
        keys=(id,),
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
    executor.execute_text(test)
    executor.environment.delete_datasource("raw_data")
    executor.execute_text(
        """ select
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
    from trilogy.parsing.common import function_to_concept

    executor.parse_text(test)
    ratio = env.concepts["ratio"]
    assert ratio.purpose == Purpose.METRIC
    assert set(x.address for x in env.concepts["survivors"].keys) == {
        "passenger.class",
    }
    assert (
        len(
            Grain(
                components=[env.concepts["survivors"], env.concepts["passenger.class"]]
            ).components
        )
        == 1
    )

    testc = function_to_concept(
        parent=env.concepts["ratio"].lineage, name="test", namespace="test"
    )
    assert set(x.address for x in testc.keys) == {
        "passenger.class",
    }

    assert (
        LooseConceptList(concepts=env.concepts["survivors"].grain.components).addresses
        == LooseConceptList(concepts=[env.concepts["passenger.class"]]).addresses
    )
    # assert  LooseConceptList(concepts=env.concepts["ratio"].grain.components).addresses ==  LooseConceptList(concepts = [env.concepts["passenger.class"]]).addresses
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
    assert results[0].surviving_size == 4


def test_demo_const():
    executor = setup_engine(debug_flag=False)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """
    const right_now <- current_datetime(); select right_now;"""

    executor.execute_text(test)[-1].fetchall()


def test_demo_rowset():
    executor = setup_engine(debug_flag=True)
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
    raw = executor.generate_sql(test)[-1]
    assert raw.count("STRING_SPLIT") == 1
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 5
    assert len(results[0]) == 3


def test_demo_duplication():
    executor = setup_engine(debug_flag=False)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """auto surviving_passenger<- filter passenger.id where passenger.survived =1; 
select 
    passenger.last_name,
    passenger.id.count,
    count(surviving_passenger) -> surviving_size
order by
    passenger.id.count desc
limit 5;"""

    # raw = executor.generate_sql(test)
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 5
    first_row = results[0]
    assert first_row.passenger_last_name == "Andersson"
    assert first_row.passenger_id_count == 9
    assert first_row.surviving_size == 2


def test_demo_suggested_answer():
    executor = setup_engine(debug_flag=False)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """auto survivor <- filter passenger.id 
where passenger.survived = 1;

property passenger.id.decade<- cast(passenger.age / 10 as int) * 10;
select 
    passenger.decade, 
    (count(survivor) by passenger.decade)/(count(passenger.id) by passenger.decade)
    ->survival_rate,
    count(passenger.id) -> bucket_size
order by passenger.decade desc
;"""
    # raw = executor.generate_sql(test)
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 10


def test_demo_suggested_answer_failing():
    executor = setup_engine(debug_flag=True)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """
auto survivor <- filter passenger.id 
where passenger.survived = 1;
select 
    count(survivor)/count(passenger.id)
    -> survival_rate_auto,
    passenger.class
order by passenger.class desc
;
"""
    env.parse(test)
    srate = env.concepts["survival_rate_auto"]
    assert srate.lineage
    assert isinstance(srate.lineage, Function)
    assert isinstance(srate.lineage, SelectGrain)
    for agg in env.concepts["survival_rate_auto"].lineage.arguments:
        assert agg.grain.components == [env.concepts["passenger.class"]]
        assert len(agg.grain.components) == 1
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 3

    assert round(results[0].survival_rate_auto, 2) == 0.24

    test = """
auto survivor <- filter passenger.id 
where passenger.survived = 1;
select 
    count(survivor)/count(passenger.id)+1
    -> survival_rate_auto_two,
    passenger.class
order by passenger.class desc
;
"""
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 3

    assert round(results[0].survival_rate_auto_two, 2) == 1.24


def test_demo_suggested_answer_failing_intentional():
    executor = setup_engine(debug_flag=True)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """
auto survivor <- filter passenger.id 
where passenger.survived = 1;
select 
    (count(survivor) by *)/(count(passenger.id) by *)
    ->survival_rate,
    count(passenger.id)->clas_count,
    passenger.class
order by passenger.class desc
;
"""
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 3


def test_demo_basic():
    executor = setup_engine(debug_flag=True)
    env = Environment()
    setup_titanic(env)
    executor.environment = env
    test = """
SELECT
    passenger.id,
    passenger.id+1 -> id_one,
    passenger.name
WHERE
    passenger.name like '%a%'
ORDER BY
    passenger.name asc
;
"""
    base = executor.parse_text(test)[-1]
    results = executor.generate_sql(base)[-1]

    row_results = executor.execute_text(test)[-1].fetchall()
    assert len(row_results) == 794

    assert (
        results.strip()
        == """
SELECT
    raw_data."passengerid" as "passenger_id",
    (raw_data."passengerid" + 1) as "id_one",
    raw_data."name" as "passenger_name"
FROM
    raw_titanic as raw_data
WHERE
     CASE WHEN raw_data."name" like '%a%' THEN True ELSE False END = True

ORDER BY 
    raw_data."name" asc
""".strip()
    )


def test_merge(base_test_env):
    executor = setup_engine(debug_flag=True)
    executor.environment = base_test_env
    cmd = """MERGE passenger.last_name, rich_info.last_name;

SELECT
    passenger.last_name,
    count(passenger.id)-> family_count,
    rich_info.net_worth_1918_dollars,
WHERE
    rich_info.net_worth_1918_dollars is not null
    and passenger.last_name is not null
ORDER BY 
    family_count desc ;"""
    results = executor.execute_text(cmd)[-1].fetchall()

    assert len(results) == 8
