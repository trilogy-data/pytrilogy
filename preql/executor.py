from typing import List
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine, CursorResult

from preql.constants import logger
from preql.core.models import Environment, ProcessedQuery, ProcessedQueryPersist
from preql.dialect.base import BaseDialect
from preql.dialect.enums import Dialects
from preql.parser import parse_text
from preql.hooks.base_hook import BaseHook


class Executor(object):
    def __init__(
        self,
        dialect: Dialects,
        engine: Engine,
        environment: Optional[Environment] = None,
        hooks: List[BaseHook] | None = None,
    ):
        self.dialect: Dialects = dialect
        self.engine = engine
        self.environment = environment or Environment()
        self.generator: BaseDialect
        self.logger = logger
        self.hooks = hooks
        if self.dialect == Dialects.BIGQUERY:
            from preql.dialect.bigquery import BigqueryDialect

            self.generator = BigqueryDialect()
        elif self.dialect == Dialects.SQL_SERVER:
            from preql.dialect.sql_server import SqlServerDialect

            self.generator = SqlServerDialect()
        elif self.dialect == Dialects.DUCK_DB:
            from preql.dialect.duckdb import DuckDBDialect

            self.generator = DuckDBDialect()
        elif self.dialect == Dialects.PRESTO:
            from preql.dialect.presto import PrestoDialect

            self.generator = PrestoDialect()
        elif self.dialect == Dialects.TRINO:
            from preql.dialect.presto import TrinoDialect

            self.generator = TrinoDialect()

        else:
            raise ValueError(f"Unsupported dialect {self.dialect}")
        self.connection = self.engine.connect()

    def execute_query(self, query: ProcessedQuery) -> CursorResult:
        """Run parsed preql query"""
        sql = self.generator.compile_statement(query)
        # connection = self.engine.connect()
        output = self.connection.execute(text(sql))
        return output

    def generate_sql(self, command: str) -> List[str]:
        """generate SQL for execution"""
        _, parsed = parse_text(command, self.environment)
        sql = self.generator.generate_queries(
            self.environment, parsed, hooks=self.hooks
        )
        output = []
        for statement in sql:
            compiled_sql = self.generator.compile_statement(statement)
            output.append(compiled_sql)
        return output

    def parse_text(self, command: str) -> List[ProcessedQuery | ProcessedQueryPersist]:
        """Process a preql text command"""
        _, parsed = parse_text(command, self.environment)
        sql = self.generator.generate_queries(
            self.environment, parsed, hooks=self.hooks
        )
        return sql

    def execute_raw_sql(self, command: str) -> CursorResult:
        """Run a command against the raw underlying
        execution engine."""
        return self.connection.execute(text(command))

    def execute_text(self, command: str) -> List[CursorResult]:
        """Run a preql text command"""
        sql = self.parse_text(command)
        output = []
        # connection = self.engine.connect()
        for statement in sql:
            compiled_sql = self.generator.compile_statement(statement)
            logger.debug(compiled_sql)
            output.append(self.connection.execute(text(compiled_sql)))
            # generalize post-run success hooks
            if isinstance(statement, ProcessedQueryPersist):
                self.environment.add_datasource(statement.datasource)
        return output
