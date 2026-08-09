# from trilogy.compiler import compile
from os.path import dirname, join

import pytest

from trilogy.core.models.author import Concept
from trilogy.core.models.environment import (
    Environment,
)
from trilogy.core.models.execute import QueryDatasource
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.parser import parse


@pytest.mark.adventureworks
def test_parsing(environment: Environment):
    with open(
        join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
    ) as f:
        file = f.read()
    SqlServerDialect()
    environment, _statements = parse(file, environment=environment)


# @pytest.mark.adventureworks_execution
# def test_finance_queries(adventureworks_engine: Executor, environment: Environment):
#     with open(
#         join(dirname(__file__), "finance_queries.preql"), "r", encoding="utf-8"
#     ) as f:
#         file = f.read()
#     generator = SqlServerDialect()
#     environment, statements = parse(file, environment=environment)
#     sql = generator.generate_queries(environment, statements)

#     for statement in sql:
#         if not isinstance(statement, (ProcessedQuery, ProcessedQueryPersist)):
#             continue
#         generator.compile_statement(statement)
#         results = adventureworks_engine.execute_query(statement)
#         assert list(results)[0] == ("Canadian Division", 8, 292174782.71999985)


def recurse_datasource(parent: QueryDatasource, depth=0):
    for x in parent.datasources:
        if isinstance(x, QueryDatasource):
            recurse_datasource(x, depth + 1)


def list_to_address(clist: list[Concept]) -> set[str]:
    return {c.address for c in clist}


# @pytest.mark.adventureworks_execution
# def test_online_sales_queries(
#     adventureworks_engine: Executor, environment: Environment
# ):
#     with open(
#         join(dirname(__file__), "online_sales_queries.preql"), "r", encoding="utf-8"
#     ) as f:
#         file = f.read()
#     generator = SqlServerDialect()
#     environment, statements = parse(file, environment=environment)
#     sql = generator.generate_queries(environment, statements)

#     for statement in sql:
#         generator.compile_statement(statement)
#         adventureworks_engine.execute_query(statement).fetchall()
