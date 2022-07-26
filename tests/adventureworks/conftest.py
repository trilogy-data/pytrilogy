from pytest import fixture
from os.path import dirname, join

from sqlalchemy.engine import create_engine, url

from preql import Executor, Dialects, Environment
from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse
from urllib.parse import quote_plus
from socket import gethostname
from os.path import dirname

@fixture(scope='session')
def adventureworks_engine():


    WORKING_STRING = f'Trusted_Connection=YES;TrustServerCertificate=YES;DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={gethostname()}\SQLEXPRESS'

    params = quote_plus(WORKING_STRING)

    #engine = sqlalchemy.create_engine('mssql://*server_name*\\SQLEXPRESS/*database_name*?trusted_connection=yes')
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    # validate connection
    engine.engine.execute('select 1').fetchall()
    executor = Executor(dialect=Dialects.SQL_SERVER, engine=engine)
    yield executor

@fixture(scope='session')
def environment():
    yield Environment({}, {}, dirname(__file__))