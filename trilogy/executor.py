from typing import List, Optional, Any, Generator
from functools import singledispatchmethod
from sqlalchemy import text
from sqlalchemy.engine import Engine, CursorResult

from trilogy.constants import logger
from trilogy.core.models import (
    Environment,
    ProcessedQuery,
    ProcessedShowStatement,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedCopyStatement,
    RawSQLStatement,
    MultiSelectStatement,
    SelectStatement,
    PersistStatement,
    ShowStatement,
    Concept,
    ConceptDeclarationStatement,
    Datasource,
    CopyStatement,
)
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.enums import Dialects
from trilogy.core.enums import IOType
from trilogy.parser import parse_text
from trilogy.hooks.base_hook import BaseHook
from pathlib import Path
from dataclasses import dataclass


@dataclass
class MockResult:
    values: list[Any]
    columns: list[str]

    def fetchall(self):
        return self.values

    def keys(self):
        return self.columns


def generate_result_set(columns: List[Concept], output_data: list[Any]) -> MockResult:
    names = [x.address.replace(".", "_") for x in columns]
    return MockResult(
        values=[dict(zip(names, [row])) for row in output_data], columns=names
    )


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
            from trilogy.dialect.bigquery import BigqueryDialect

            self.generator = BigqueryDialect()
        elif self.dialect == Dialects.SQL_SERVER:
            from trilogy.dialect.sql_server import SqlServerDialect

            self.generator = SqlServerDialect()
        elif self.dialect == Dialects.DUCK_DB:
            from trilogy.dialect.duckdb import DuckDBDialect

            self.generator = DuckDBDialect()
        elif self.dialect == Dialects.PRESTO:
            from trilogy.dialect.presto import PrestoDialect

            self.generator = PrestoDialect()
        elif self.dialect == Dialects.TRINO:
            from trilogy.dialect.presto import TrinoDialect

            self.generator = TrinoDialect()
        elif self.dialect == Dialects.POSTGRES:
            from trilogy.dialect.postgres import PostgresDialect

            self.generator = PostgresDialect()
        elif self.dialect == Dialects.SNOWFLAKE:

            from trilogy.dialect.snowflake import SnowflakeDialect

            self.generator = SnowflakeDialect()
        else:
            raise ValueError(f"Unsupported dialect {self.dialect}")
        self.connection = self.engine.connect()

    def execute_statement(self, statement) -> Optional[CursorResult]:
        if not isinstance(
            statement,
            (
                ProcessedQuery,
                ProcessedShowStatement,
                ProcessedQueryPersist,
                ProcessedCopyStatement,
            ),
        ):
            return None
        return self.execute_query(statement)

    @singledispatchmethod
    def execute_query(self, query) -> CursorResult:
        raise NotImplementedError("Cannot execute type {}".format(type(query)))

    @execute_query.register
    def _(self, query: ConceptDeclarationStatement) -> CursorResult:
        concept = query.concept
        return MockResult(
            [
                {
                    "address": concept.address,
                    "type": concept.datatype.value,
                    "purpose": concept.purpose.value,
                    "derivation": concept.derivation.value,
                }
            ],
            ["address", "type", "purpose", "derivation"],
        )

    @execute_query.register
    def _(self, query: Datasource) -> CursorResult:

        return MockResult(
            [
                {
                    "name": query.name,
                }
            ],
            ["name"],
        )

    @execute_query.register
    def _(self, query: str) -> CursorResult:

        return self.execute_text(query)[-1]

    @execute_query.register
    def _(self, query: SelectStatement) -> CursorResult:
        sql = self.generator.generate_queries(
            self.environment, [query], hooks=self.hooks
        )
        return self.execute_query(sql[0])

    @execute_query.register
    def _(self, query: PersistStatement) -> CursorResult:
        sql = self.generator.generate_queries(
            self.environment, [query], hooks=self.hooks
        )
        return self.execute_query(sql[0])

    @execute_query.register
    def _(self, query: RawSQLStatement) -> CursorResult:
        return self.execute_raw_sql(query.text)

    @execute_query.register
    def _(self, query: ShowStatement) -> CursorResult:
        sql = self.generator.generate_queries(
            self.environment, [query], hooks=self.hooks
        )
        return self.execute_query(sql[0])

    @execute_query.register
    def _(self, query: ProcessedShowStatement) -> CursorResult:
        return generate_result_set(
            query.output_columns,
            [
                self.generator.compile_statement(x)
                for x in query.output_values
                if isinstance(x, ProcessedQuery)
            ],
        )

    @execute_query.register
    def _(self, query: ProcessedRawSQLStatement) -> CursorResult:
        return self.execute_raw_sql(query.text)

    @execute_query.register
    def _(self, query: ProcessedQuery) -> CursorResult:
        sql = self.generator.compile_statement(query)
        # connection = self.engine.connect()
        output = self.connection.execute(text(sql))
        return output

    @execute_query.register
    def _(self, query: ProcessedQueryPersist) -> CursorResult:

        sql = self.generator.compile_statement(query)

        output = self.connection.execute(text(sql))
        self.environment.add_datasource(query.datasource)
        return output

    @execute_query.register
    def _(self, query: ProcessedCopyStatement) -> CursorResult:
        sql = self.generator.compile_statement(query)
        output: CursorResult = self.connection.execute(text(sql))
        if query.target_type == IOType.CSV:
            import csv

            with open(query.target, "w", newline="", encoding="utf-8") as f:
                outcsv = csv.writer(f)
                outcsv.writerow(output.keys())
                outcsv.writerows(output)
        else:
            raise NotImplementedError(f"Unsupported IOType {query.target_type}")
        # now return the query we ran through IO
        # TODO: instead return how many rows were written?
        return generate_result_set(
            query.output_columns,
            [self.generator.compile_statement(query)],
        )

    @singledispatchmethod
    def generate_sql(self, command) -> list[str]:
        raise NotImplementedError(
            "Cannot generate sql for type {}".format(type(command))
        )

    @generate_sql.register  # type: ignore
    def _(self, command: ProcessedQuery) -> List[str]:
        output = []
        compiled_sql = self.generator.compile_statement(command)
        output.append(compiled_sql)
        return output

    @generate_sql.register  # type: ignore
    def _(self, command: MultiSelectStatement) -> List[str]:
        output = []
        sql = self.generator.generate_queries(
            self.environment, [command], hooks=self.hooks
        )
        for statement in sql:
            compiled_sql = self.generator.compile_statement(statement)
            output.append(compiled_sql)

        output.append(compiled_sql)
        return output

    @generate_sql.register  # type: ignore
    def _(self, command: SelectStatement) -> List[str]:
        output = []
        sql = self.generator.generate_queries(
            self.environment, [command], hooks=self.hooks
        )
        for statement in sql:
            compiled_sql = self.generator.compile_statement(statement)
            output.append(compiled_sql)
        return output

    @generate_sql.register  # type: ignore
    def _(self, command: str) -> List[str]:
        """generate SQL for execution"""
        _, parsed = parse_text(command, self.environment)
        generatable = [
            x for x in parsed if isinstance(x, (SelectStatement, PersistStatement))
        ]
        sql = self.generator.generate_queries(
            self.environment, generatable, hooks=self.hooks
        )
        output = []
        for statement in sql:
            if isinstance(statement, ProcessedShowStatement):
                continue
            compiled_sql = self.generator.compile_statement(statement)
            output.append(compiled_sql)
        return output

    def parse_text(
        self, command: str, persist: bool = False
    ) -> List[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
        | ProcessedCopyStatement
    ]:

        return list(self.parse_text_generator(command, persist=persist))

    def parse_text_generator(self, command: str, persist: bool = False) -> Generator[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
        | ProcessedCopyStatement,
        None,
        None,
    ]:
        """Process a preql text command"""
        _, parsed = parse_text(command, self.environment)
        generatable = [
            x
            for x in parsed
            if isinstance(
                x,
                (
                    SelectStatement,
                    PersistStatement,
                    MultiSelectStatement,
                    ShowStatement,
                    RawSQLStatement,
                    CopyStatement,
                ),
            )
        ]
        while generatable:
            t = generatable.pop(0)
            x = self.generator.generate_queries(
                self.environment, [t], hooks=self.hooks
            )[0]
            if persist and isinstance(x, ProcessedQueryPersist):
                self.environment.add_datasource(x.datasource)
            yield x

    def execute_raw_sql(
        self, command: str, variables: dict | None = None
    ) -> CursorResult:
        """Run a command against the raw underlying
        execution engine"""
        if variables:
            return self.connection.execute(text(command), variables)
        return self.connection.execute(
            text(command),
        )

    def execute_text(self, command: str) -> List[CursorResult]:
        """Run a preql text command"""
        output = []
        # connection = self.engine.connect()
        for statement in self.parse_text_generator(command):
            if isinstance(statement, ProcessedShowStatement):
                output.append(
                    generate_result_set(
                        statement.output_columns,
                        [
                            self.generator.compile_statement(x)
                            for x in statement.output_values
                            if isinstance(x, ProcessedQuery)
                        ],
                    )
                )
                continue
            output.append(self.execute_query(statement))
        return output

    def execute_file(self, file: str | Path) -> List[CursorResult]:
        file = Path(file)
        with open(file, "r") as f:
            command = f.read()
        return self.execute_text(command)
