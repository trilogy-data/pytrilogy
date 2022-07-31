# from preql.compiler import compile
from os.path import dirname, join

from preql.dialect.sql_server import SqlServerDialect
from preql.parser import parse


def test_finance_queries(adventureworks_engine, environment):
    with open(join(dirname(__file__), 'finance_queries.preql'), 'r', encoding='utf-8') as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        sql = generator.compile_statement(statement)
        results = adventureworks_engine.execute_query(statement)


def test_online_sales_queries(adventureworks_engine, environment):
    with open(join(dirname(__file__), 'online_sales_queries.preql'), 'r', encoding='utf-8') as f:
        file = f.read()
    generator = SqlServerDialect()
    environment, statements = parse(file, environment=environment)
    sql = generator.generate_queries(environment, statements)

    for statement in sql:
        sql = generator.compile_statement(statement)
        results = adventureworks_engine.execute_query(statement)

