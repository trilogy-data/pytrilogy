from dataclasses import dataclass
from functools import singledispatchmethod
from pathlib import Path
from typing import Any, Generator, List, Optional

from sqlalchemy import text

from trilogy.constants import MagicConstants, Rendering, logger
from trilogy.core.enums import FunctionType, Granularity, IOType, ValidationScope
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
    ValidateStatement,
)
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedCopyStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedShowStatement,
    ProcessedStaticValueOutput,
    ProcessedValidateStatement,
)
from trilogy.core.validation.common import (
    ValidationTest,
)
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.enums import Dialects
from trilogy.engine import ExecutionEngine, ResultProtocol
from trilogy.hooks.base_hook import BaseHook
from trilogy.parser import parse_text
from trilogy.render import get_dialect_generator


@dataclass
class MockResult(ResultProtocol):
    values: list[Any]
    columns: list[str]

    def __init__(self, values: list[Any], columns: list[str]):
        processed = []
        for x in values:
            if isinstance(x, dict):
                processed.append(MockResultRow(x))
            else:
                processed.append(x)
        self.columns = columns
        self.values = processed

    def __iter__(self):
        while self.values:
            yield self.values.pop(0)

    def fetchall(self):
        return self.values

    def fetchone(self):
        if self.values:
            return self.values.pop(0)
        return None

    def fetchmany(self, size: int):
        rval = self.values[:size]
        self.values = self.values[size:]
        return rval

    def keys(self):
        return self.columns


@dataclass
class MockResultRow:
    _values: dict[str, Any]

    def __str__(self) -> str:
        return str(self._values)

    def __repr__(self) -> str:
        return repr(self._values)

    def __getattr__(self, name: str) -> Any:
        if name in self._values:
            return self._values[name]
        return super().__getattribute__(name)

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def values(self):
        return self._values.values()

    def keys(self):
        return self._values.keys()


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
        statement: PROCESSED_STATEMENT_TYPES,
    ) -> Optional[ResultProtocol]:
        if not isinstance(statement, PROCESSED_STATEMENT_TYPES):
            return None
        return self.execute_query(statement)

    @singledispatchmethod
    def execute_query(self, query) -> ResultProtocol | None:
        raise NotImplementedError("Cannot execute type {}".format(type(query)))

    @execute_query.register
    def _(self, query: ConceptDeclarationStatement) -> ResultProtocol | None:
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
    def _(self, query: Datasource) -> ResultProtocol | None:
        return MockResult(
            [
                {
                    "name": query.name,
                }
            ],
            ["name"],
        )

    @execute_query.register
    def _(self, query: str) -> ResultProtocol | None:
        results = self.execute_text(query)
        if results:
            return results[-1]
        return None

    @execute_query.register
    def _(self, query: SelectStatement) -> ResultProtocol | None:
        sql = self.generator.generate_queries(
            self.environment, [query], hooks=self.hooks
        )
        return self.execute_query(sql[0])

    @execute_query.register
    def _(self, query: PersistStatement) -> ResultProtocol | None:
        sql = self.generator.generate_queries(
            self.environment, [query], hooks=self.hooks
        )
        return self.execute_query(sql[0])

    @execute_query.register
    def _(self, query: RawSQLStatement) -> ResultProtocol | None:
        return self.execute_raw_sql(query.text)

    @execute_query.register
    def _(self, query: ShowStatement) -> ResultProtocol | None:
        sql = self.generator.generate_queries(
            self.environment, [query], hooks=self.hooks
        )
        return self.execute_query(sql[0])

    @execute_query.register
    def _(self, query: ProcessedShowStatement) -> ResultProtocol | None:
        return generate_result_set(
            query.output_columns,
            [
                self.generator.compile_statement(x)
                for x in query.output_values
                if isinstance(x, ProcessedQuery)
            ],
        )

    def _raw_validation_to_result(
        self, raw: list[ValidationTest]
    ) -> Optional[ResultProtocol]:
        if not raw:
            return None
        output = []
        for row in raw:
            output.append(
                {
                    "check_type": row.check_type.value,
                    "expected": row.expected,
                    "result": str(row.result) if row.result else None,
                    "ran": row.ran,
                    "query": row.query if row.query else "",
                }
            )
        return MockResult(output, ["check_type", "expected", "result", "ran", "query"])

    @execute_query.register
    def _(self, query: ProcessedValidateStatement) -> ResultProtocol | None:
        results = self.validate_environment(query.scope, query.targets)
        return self._raw_validation_to_result(results)

    @execute_query.register
    def _(self, query: ImportStatement) -> ResultProtocol | None:
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
    def _(self, query: MergeStatementV2) -> ResultProtocol | None:
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
    def _(self, query: ProcessedRawSQLStatement) -> ResultProtocol | None:
        return self.execute_raw_sql(query.text)

    @execute_query.register
    def _(self, query: ProcessedQuery) -> ResultProtocol | None:
        sql = self.generator.compile_statement(query)
        output = self.execute_raw_sql(sql, local_concepts=query.local_concepts)
        return output

    @execute_query.register
    def _(self, query: ProcessedQueryPersist) -> ResultProtocol | None:
        sql = self.generator.compile_statement(query)

        output = self.execute_raw_sql(sql, local_concepts=query.local_concepts)
        self.environment.add_datasource(query.datasource)
        return output

    @execute_query.register
    def _(self, query: ProcessedCopyStatement) -> ResultProtocol | None:
        sql = self.generator.compile_statement(query)
        output: ResultProtocol = self.execute_raw_sql(
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
    ) -> list[PROCESSED_STATEMENT_TYPES]:
        return list(self.parse_file_generator(file, persist=persist))

    def parse_file_generator(
        self, file: str | Path, persist: bool = False
    ) -> Generator[
        PROCESSED_STATEMENT_TYPES,
        None,
        None,
    ]:
        file = Path(file)
        with open(file, "r") as f:
            command = f.read()
            return self.parse_text_generator(command, persist=persist, root=file)

    def parse_text(
        self, command: str, persist: bool = False, root: Path | None = None
    ) -> List[PROCESSED_STATEMENT_TYPES]:
        return list(self.parse_text_generator(command, persist=persist, root=root))

    def parse_text_generator(
        self, command: str, persist: bool = False, root: Path | None = None
    ) -> Generator[
        PROCESSED_STATEMENT_TYPES,
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
                    ValidateStatement,
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

    def _atom_to_value(self, val: Any) -> Any:
        if val == MagicConstants.NULL:
            return None
        return val

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
                return [self._atom_to_value(x) for x in rval]
            if isinstance(rval, MapWrapper):
                # duckdb expects maps in this format as variables
                if self.dialect == Dialects.DUCK_DB:
                    return {
                        "key": [self._atom_to_value(x) for x in rval],
                        "value": [self._atom_to_value(rval[x]) for x in rval],
                    }
                return {k: self._atom_to_value(v) for k, v in rval.items()}
            # if isinstance(rval, ConceptRef):
            #     return self._concept_to_value(self.environment.concepts[rval.address], local_concepts=local_concepts)
            return rval
        else:
            results = self.execute_query(f"select {concept.name} limit 1;")
            if results:
                fetcher = results.fetchone()
                if fetcher:
                    return fetcher[0]
            return None

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
        command: str | Path,
        variables: dict | None = None,
        local_concepts: dict[str, Concept] | None = None,
    ) -> ResultProtocol:
        """Run a command against the raw underlying
        execution engine."""
        final_params = None
        if isinstance(command, Path):
            with open(command, "r") as f:
                command = f.read()
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
    ) -> List[ResultProtocol]:
        """Run a trilogy query expressed as text."""
        output: list[ResultProtocol] = []
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
                    elif isinstance(x, ProcessedValidateStatement):
                        raw = self.validate_environment(
                            x.scope, x.targets, generate_only=True
                        )
                        results = self._raw_validation_to_result(raw)
                        if results:
                            output.append(results)
                    else:
                        raise NotImplementedError(
                            f"Cannot show type {type(x)} in show statement"
                        )
                continue
            if non_interactive:
                if not isinstance(
                    statement, (ProcessedCopyStatement, ProcessedQueryPersist)
                ):
                    continue
            result = self.execute_statement(statement)
            if result:
                output.append(result)
        return output

    def execute_file(
        self, file: str | Path, non_interactive: bool = False
    ) -> List[ResultProtocol]:
        file = Path(file)
        with open(file, "r") as f:
            command = f.read()
        return self.execute_text(command, non_interactive=non_interactive)

    def validate_environment(
        self,
        scope: ValidationScope = ValidationScope.ALL,
        targets: Optional[List[str]] = None,
        generate_only: bool = False,
    ) -> list[ValidationTest]:
        from trilogy.core.validation.environment import validate_environment

        return validate_environment(
            self.environment, self, scope, targets, generate_only
        )
