from pytest import fixture
from os.path import dirname, abspath
import os

from sqlalchemy.engine import create_engine, url

from preql import Executor, Dialects, Environment
from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse
from urllib.parse import quote_plus
import socket
import time

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
def db_must():
    WORKING_STRING = f'TrustServerCertificate=YES;DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost;Uid=sa;Pwd=ThisIsAReallyCoolPassword123'
    if not can_bind("localhost", 1433):
        x = os.system("docker run -p 1433:1433 --name pyreql-test-sqlserver -d  --rm pyreql-test-sqlserver")
        if x != 0:
            raise TestDependencyError("Error starting database")
        time.sleep(5)
    yield None
    x = os.system("docker stop pyreql-test-sqlserver")
    if x != 0:
        raise TestDependencyError("Error stopping database")



@fixture(scope='session')
def adventureworks_engine(db_must):


    WORKING_STRING = f'TrustServerCertificate=YES;DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost;Uid=sa;Pwd=ThisIsAReallyCoolPassword123'

    params = quote_plus(WORKING_STRING)

    #engine = sqlalchemy.create_engine('mssql://*server_name*\\SQLEXPRESS/*database_name*?trusted_connection=yes')
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    # validate connection
    engine.engine.execute('select 1').fetchall()
    executor = Executor(dialect=Dialects.SQL_SERVER, engine=engine)
    yield executor

@fixture(scope='session')
def environment():
    yield Environment({}, {}, dirname(ENV_PATH), dirname(ENV_PATH))