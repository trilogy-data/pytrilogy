from os.path import dirname, abspath
from typing import Generator

from pytest import fixture
from sqlalchemy import text
from sqlalchemy.engine import create_engine

from preql import Executor, Dialects, parse, Environment
from preql.engine import ExecutionEngine, EngineConnection, EngineResult
from preql.hooks.query_debugger import DebuggingHook

ENV_PATH = abspath(__file__)


@fixture(scope="session")
def environment():
    yield Environment(working_path=dirname(ENV_PATH))


@fixture(scope="session")
def duckdb_model(environment):
    text = """
key item string;
property item.value float;
property item.discount_value float;
key store_id int;
property <item, store_id>.count int;

#metric store_total_count <- sum(count) by item, store_id;
#metric total_count <- sum(store_total_count);
metric total_count <- sum(count);

metric total_count_per_product <- sum(count) by item;

metric avg_count_per_product <- avg(total_count_per_product);

datasource fact_items (
    item:item,
    store_id:store_id,
    value:value,
    count:count,
    )
    grain (item, store_id)
    address items
;

datasource fact_discount (
    item:Partial[item],
    value:discount_value,
    )
    grain (item)
    address items_extra_discount
;


"""
    environment, statements = parse(text, environment=environment)
    yield environment


@fixture(scope="session")
def duckdb_engine(duckdb_model) -> Generator[Executor, None, None]:
    engine = create_engine("duckdb:///:memory:", future=True)

    with engine.connect() as connection:
        connection.execute(
            text(
                "CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER, store_id INTEGER)"
            )
        )
        connection.commit()
        # insert two items into the table
        connection.execute(
            text(
                "INSERT INTO items VALUES ('jeans', 20.0, 1, 1), ('hammer', 42.2, 2,1 ), ('hammer', 42.2, 2,2 )"
            )
        )
        connection.commit()
        # validate connection
        connection.execute(text("select 1")).one_or_none()

        connection.execute(
            text(
                "CREATE TABLE items_extra_discount(item VARCHAR, value DECIMAL(10,2) )"
            )
        )

        connection.commit()
        # insert extra items
        connection.execute(
            text("INSERT INTO items_extra_discount VALUES ('jeans', -10.0)")
        )
        connection.commit()
        connection.execute(text("select 1")).one_or_none()

    executor = Executor(
        dialect=Dialects.DUCK_DB,
        engine=engine,
        environment=duckdb_model,
        hooks=[DebuggingHook()],
    )
    yield executor


@fixture(scope="session")
def expected_results():
    yield {"total_count": 5, "avg_count_per_product": 2.5, "converted_total_count": 10}


@fixture(scope="session")
def presto_model(environment):
    text = """
const pi <-3.14;
"""
    environment, statements = parse(text, environment=environment)
    yield environment


class PrestoEngineResult(EngineResult):
    def __init__(self):
        pass

    def fetchall(self) -> list[tuple]:
        # hardcoded
        return [(1,)]


class PrestoEngineConnection(EngineConnection):
    def __init__(self):
        pass

    def execute(self, statement: str) -> EngineResult:
        return PrestoEngineResult()


class PrestoEngine(ExecutionEngine):
    pass

    def connect(self) -> EngineConnection:
        return PrestoEngineConnection()


@fixture(scope="session")
def presto_engine(presto_model) -> Generator[Executor, None, None]:
    engine = PrestoEngine()

    executor = Executor(
        dialect=Dialects.DUCK_DB, engine=engine, environment=presto_model
    )
    yield executor


class PostgresEngine(ExecutionEngine):
    """Mock since we won't have the engine available in testing"""

    pass

    def connect(self) -> EngineConnection:
        return PrestoEngineConnection()


@fixture(scope="session")
def postgres_engine(presto_model) -> Generator[Executor, None, None]:
    engine = PostgresEngine()

    executor = Executor(
        dialect=Dialects.POSTGRES, engine=engine, environment=presto_model
    )
    yield executor
