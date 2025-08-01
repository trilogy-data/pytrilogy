from dataclasses import dataclass
from functools import singledispatchmethod
from pathlib import Path
from typing import Any, Generator, List, Optional, Protocol

from sqlalchemy import text
from sqlalchemy.engine import CursorResult

from trilogy.constants import Rendering, logger
from trilogy.core.enums import FunctionType, Granularity, IOType
from trilogy.core.models.author import Concept, ConceptRef, Function
from trilogy.core.models.build import BuildFunction
from trilogy.core.models.core import ListWrapper, MapWrapper
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    CopyStatement,
    ImportStatement,
    MergeStatementV2,
    MultiSelectStatement,
    PersistStatement,
    RawSQLStatement,
    SelectStatement,
    ShowStatement,
)
from trilogy.core.statements.execute import (
    ProcessedCopyStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedShowStatement,
    ProcessedStaticValueOutput,
)
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.enums import Dialects
from trilogy.engine import ExecutionEngine
from trilogy.hooks.base_hook import BaseHook
from trilogy.parser import parse_text
from trilogy.render import get_dialect_generator


class ResultProtocol(Protocol):
    values: List[Any]
    columns: List[str]

    def fetchall(self) -> List[Any]: ...

    def keys(self) -> List[str]: ...


@dataclass
class MockResult:
    values: list[Any]
    columns: list[str]

    def fetchall(self):
        return self.values

    def keys(self):
        return self.columns


def generate_result_set(
    columns: List[ConceptRef], output_data: list[Any]
) -> MockResult:
    names = [x.address.replace(".", "_") for x in columns]
    return MockResult(
        values=[dict(zip(names, [row])) for row in output_data], columns=names
    )


class Executor(object):
    def __init__(
        self,
        dialect: Dialects,
        engine: ExecutionEngine,
        environment: Optional[Environment] = None,
        rendering: Rendering | None = None,
        hooks: List[BaseHook] | None = None,
    ):
        self.dialect: Dialects = dialect
        self.engine = engine
        self.environment = environment or Environment()
        self.generator: BaseDialect
        self.logger = logger
        self.hooks = hooks
        self.generator = get_dialect_generator(self.dialect, rendering)
        self.connection = self.engine.connect()
        # TODO: make generic
        if self.dialect == Dialects.DATAFRAME:
            self.engine.setup(self.environment, self.connection)

    def execute_statement(
        self,
        statement: (
            ProcessedQuery
            | ProcessedCopyStatement
            | ProcessedRawSQLStatement
            | ProcessedQueryPersist
            | ProcessedShowStatement
        ),
    ) -> Optional[CursorResult]:
        if not isinstance(
            statement,
            (
                ProcessedQuery,
                ProcessedShowStatement,
                ProcessedQueryPersist,
                ProcessedCopyStatement,
                ProcessedRawSQLStatement,
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
    def _(self, query: str) -> CursorResult | None:
        results = self.execute_text(query)
        if results:
            return results[-1]
        return None

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
    def _(self, query: ImportStatement) -> CursorResult:
        return MockResult(
            [
                {
                    "path": query.path,
                    "alias": query.alias,
                }
            ],
            ["path", "alias"],
        )

    @execute_query.register
    def _(self, query: MergeStatementV2) -> CursorResult:
        for concept in query.sources:
            self.environment.merge_concept(
                concept, query.targets[concept.address], modifiers=query.modifiers
            )

        return MockResult(
            [
                {
                    "sources": ",".join([x.address for x in query.sources]),
                    "targets": ",".join([x.address for _, x in query.targets.items()]),
                }
            ],
            ["source", "target"],
        )

    @execute_query.register
    def _(self, query: ProcessedRawSQLStatement) -> CursorResult:
        return self.execute_raw_sql(query.text)

    @execute_query.register
    def _(self, query: ProcessedQuery) -> CursorResult:
        sql = self.generator.compile_statement(query)
        output = self.execute_raw_sql(sql, local_concepts=query.local_concepts)
        return output

    @execute_query.register
    def _(self, query: ProcessedQueryPersist) -> CursorResult:
        sql = self.generator.compile_statement(query)

        output = self.execute_raw_sql(sql, local_concepts=query.local_concepts)
        self.environment.add_datasource(query.datasource)
        return output

    @execute_query.register
    def _(self, query: ProcessedCopyStatement) -> CursorResult:
        sql = self.generator.compile_statement(query)
        output: CursorResult = self.execute_raw_sql(
            sql, local_concepts=query.local_concepts
        )
        if query.target_type == IOType.CSV:
            import csv

            with open(query.target, "w", newline="", encoding="utf-8") as f:
                outcsv = csv.writer(f)
                outcsv.writerow(output.keys())
                outcsv.writerows(output)
        else:
            raise NotImplementedError(f"Unsupported IO Type {query.target_type}")
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

    def parse_file(
        self, file: str | Path, persist: bool = False
    ) -> list[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
        | ProcessedCopyStatement,
    ]:
        return list(self.parse_file_generator(file, persist=persist))

    def parse_file_generator(
        self, file: str | Path, persist: bool = False
    ) -> Generator[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
        | ProcessedCopyStatement,
        None,
        None,
    ]:
        file = Path(file)
        with open(file, "r") as f:
            command = f.read()
            return self.parse_text_generator(command, persist=persist, root=file)

    def parse_text(
        self, command: str, persist: bool = False, root: Path | None = None
    ) -> List[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
        | ProcessedCopyStatement
    ]:
        return list(self.parse_text_generator(command, persist=persist, root=root))

    def parse_text_generator(
        self, command: str, persist: bool = False, root: Path | None = None
    ) -> Generator[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
        | ProcessedCopyStatement,
        None,
        None,
    ]:
        """Process a preql text command"""
        _, parsed = parse_text(command, self.environment, root=root)
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

            yield x

            if persist and isinstance(x, ProcessedQueryPersist):
                self.environment.add_datasource(x.datasource)

    def _concept_to_value(
        self,
        concept: Concept,
        local_concepts: dict[str, Concept] | None = None,
    ) -> Any:
        if not concept.granularity == Granularity.SINGLE_ROW:
            raise SyntaxError(f"Cannot bind non-singleton concept {concept.address}")
        # TODO: to get rid of function here - need to figure out why it's getting passed in
        if (
            isinstance(concept.lineage, (BuildFunction, Function))
            and concept.lineage.operator == FunctionType.CONSTANT
        ):
            rval = concept.lineage.arguments[0]
            if isinstance(rval, ListWrapper):
                return [x for x in rval]
            if isinstance(rval, MapWrapper):
                # duckdb expects maps in this format as variables
                if self.dialect == Dialects.DUCK_DB:
                    return {"key": [x for x in rval], "value": [rval[x] for x in rval]}
                return {k: v for k, v in rval.items()}
            # if isinstance(rval, ConceptRef):
            #     return self._concept_to_value(self.environment.concepts[rval.address], local_concepts=local_concepts)
            return rval
        else:
            results = self.execute_query(f"select {concept.name} limit 1;").fetchone()
        if not results:
            return None
        return results[0]

    def _hydrate_param(
        self, param: str, local_concepts: dict[str, Concept] | None = None
    ) -> Any:
        matched = [
            v
            for v in self.environment.concepts.values()
            if v.safe_address == param or v.address == param
        ]
        if local_concepts and not matched:
            matched = [
                v
                for v in local_concepts.values()
                if v.safe_address == param or v.address == param
            ]
        if not matched:
            raise SyntaxError(f"No concept found for parameter {param}")

        concept: Concept = matched.pop()
        return self._concept_to_value(concept, local_concepts=local_concepts)

    def execute_raw_sql(
        self,
        command: str,
        variables: dict | None = None,
        local_concepts: dict[str, Concept] | None = None,
    ) -> CursorResult:
        """Run a command against the raw underlying
        execution engine."""
        final_params = None
        q = text(command)
        if variables:
            final_params = variables
        else:
            params = q.compile().params
            if params:
                final_params = {
                    x: self._hydrate_param(x, local_concepts=local_concepts)
                    for x in params
                }

        if final_params:
            return self.connection.execute(text(command), final_params)
        return self.connection.execute(
            text(command),
        )

    def execute_text(
        self, command: str, non_interactive: bool = False
    ) -> List[CursorResult]:
        """Run a trilogy query expressed as text."""
        output = []
        # connection = self.engine.connect()
        for statement in self.parse_text_generator(command):
            if isinstance(statement, ProcessedShowStatement):
                for x in statement.output_values:
                    if isinstance(x, ProcessedStaticValueOutput):
                        output.append(
                            generate_result_set(statement.output_columns, x.values)
                        )
                    elif isinstance(x, ProcessedQuery):
                        output.append(
                            generate_result_set(
                                statement.output_columns,
                                [self.generator.compile_statement(x)],
                            )
                        )
                continue
            if non_interactive:
                if not isinstance(
                    statement, (ProcessedCopyStatement, ProcessedQueryPersist)
                ):
                    continue
            output.append(self.execute_query(statement))
        return output

    def execute_file(
        self, file: str | Path, non_interactive: bool = False
    ) -> List[CursorResult]:
        file = Path(file)
        with open(file, "r") as f:
            command = f.read()
        return self.execute_text(command, non_interactive=non_interactive)
