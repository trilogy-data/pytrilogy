import os
import socket
import time
from enum import Enum
from os.path import dirname, abspath
from socket import gethostname
from urllib.parse import quote_plus

from pytest import fixture
from sqlalchemy.engine import create_engine
from sqlalchemy import text
from preql.constants import logger
from logging import DEBUG

logger.setLevel(DEBUG)
from preql import Executor, Dialects, Environment


class TestConfig(Enum):
    LOCAL = 1
    DOCKER = 2


class TestDependencyError(Exception):
    pass


ENV_PATH = abspath(__file__)


# Just assume that if there is a service listening on localhost:1433
# your test sql server database is present.
def can_bind(hostname, port):
    sock = None
    try:
        sock = socket.create_connection((hostname, port), timeout=5)
    except Exception as e:
        return False
    finally:
        if sock:
            sock.close()

    return True


@fixture(scope="session")
def local_express_flag():
    if os.environ.get("PYPREQL_INTEGRATION_SQLSERVER_EXPRESS", None) == "true":
        return create_engine(
            "mssql://*server_name*\\SQLEXPRESS/*database_name*?trusted_connection=yes;DRIVER={ODBC Driver 18 for SQL Server};"
        )
    else:
        return False


@fixture(scope="session")
def db_must(local_express_flag):
    # if a local instance can be found
    # use that instead of managing docker
    if local_express_flag:
        yield TestConfig.LOCAL
        return
    if not can_bind("localhost", 1433):
        x = os.system(
            "docker run -p 1433:1433 --name pyreql-test-sqlserver -d  --rm pyreql-test-sqlserver"
        )
        if x != 0:
            raise TestDependencyError("Error starting database")
        time.sleep(5)
    yield TestConfig.DOCKER
    x = os.system("docker stop pyreql-test-sqlserver")
    if x != 0:
        raise TestDependencyError("Error stopping database")


@fixture(scope="session")
def adventureworks_engine(db_must):
    if db_must == TestConfig.DOCKER:
        connection_string = f"TrustServerCertificate=YES;DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost;Uid=sa;Pwd=ThisIsAReallyCoolPassword123"
    elif db_must == TestConfig.LOCAL:
        connection_string = f"Trusted_Connection=YES;TrustServerCertificate=YES;DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={gethostname()}\\SQLEXPRESS"
    else:
        raise TestDependencyError("Unknown DB configuration")
    params = quote_plus(connection_string)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, future=True)
    # validate connection
    with engine.connect() as connection:
        results = connection.execute(text("select 1")).one_or_none()
        assert results == (1,)
    executor = Executor(dialect=Dialects.SQL_SERVER, engine=engine)
    yield executor


@fixture(scope="session")
def environment():
    yield Environment(namespace=dirname(ENV_PATH), working_path=dirname(ENV_PATH))
