from sqlalchemy.engine import create_engine, Engine
from typing import List, Any
from preql.dialect.enums import Dialects
from preql.core.models import Environment, ProcessedQuery
from preql.parser import parse_text
from typing import Optional


class Executor(object):
    def __init__(self, dialect: Dialects, engine: Engine, environment: Optional[Environment] = None):
        self.dialect = dialect
        self.engine = engine
        self.environment = environment or Environment({}, {})
        if self.dialect == Dialects.BIGQUERY:
            from preql.dialect.bigquery import BigqueryDialect
            self.generator = BigqueryDialect()
        elif self.dialect == Dialects.SQL_SERVER:
            from preql.dialect.sql_server import SqlServerDialect
            self.generator = SqlServerDialect()
        else:
            raise ValueError(f"Unsupported dialect {self.dialect}")

    def execute_query(self, query:ProcessedQuery):
        sql = self.generator.compile_statement(query)
        output = self.engine.execute(sql).fetchall()
        return output


    def execute_text(self, command: str):
        _, parsed = parse_text(command, self.environment)
        sql = self.generator.generate_queries(self.environment, parsed)
        output = None
        for statement in sql:
            sql = self.generator.compile_statement(statement)
            output = self.engine.execute(sql).fetchall()
        return output
