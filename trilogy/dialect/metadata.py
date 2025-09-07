from dataclasses import dataclass
from typing import Any, List, Optional

from trilogy.core.models.author import ConceptRef
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ImportStatement,
    MergeStatementV2,
)
from trilogy.core.statements.execute import (
    ProcessedShowStatement,
    ProcessedStaticValueOutput,
    ProcessedValidateStatement,
)
from trilogy.core.validation.common import ValidationTest
from trilogy.dialect.base import BaseDialect
from trilogy.engine import ResultProtocol


@dataclass
class MockResult(ResultProtocol):
    values: list["MockResultRow"]
    columns: list[str]

    def __init__(self, values: list[Any], columns: list[str]):
        processed: list[MockResultRow] = []
        for x in values:
            if isinstance(x, dict):
                processed.append(MockResultRow(x))
            elif isinstance(x, MockResultRow):
                processed.append(x)
            else:
                raise ValueError(
                    f"Cannot process value of type {type(x)} in MockResult"
                )
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

    def as_dict(self):
        return [x.as_dict() if isinstance(x, MockResultRow) else x for x in self.values]


@dataclass
class MockResultRow:
    _values: dict[str, Any]

    def as_dict(self):
        return self._values

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
    """Generate a mock result set from columns and output data."""
    names = [x.address.replace(".", "_") for x in columns]
    return MockResult(
        values=[dict(zip(names, [row])) for row in output_data], columns=names
    )


def handle_concept_declaration(query: ConceptDeclarationStatement) -> MockResult:
    """Handle concept declaration statements without execution."""
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


def handle_datasource(query: Datasource) -> MockResult:
    """Handle datasource queries without execution."""
    return MockResult(
        [
            {
                "name": query.name,
            }
        ],
        ["name"],
    )


def handle_import_statement(query: ImportStatement) -> MockResult:
    """Handle import statements without execution."""
    return MockResult(
        [
            {
                "path": query.path,
                "alias": query.alias,
            }
        ],
        ["path", "alias"],
    )


def handle_merge_statement(
    query: MergeStatementV2, environment: Environment
) -> MockResult:
    """Handle merge statements by updating environment and returning result."""
    for concept in query.sources:
        environment.merge_concept(
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


def handle_processed_show_statement(
    query: ProcessedShowStatement, compiled_statements: list[str]
) -> MockResult:
    """Handle processed show statements without execution."""

    return generate_result_set(query.output_columns, compiled_statements)


def raw_validation_to_result(
    raw: list[ValidationTest], generator: Optional[BaseDialect] = None
) -> Optional[MockResult]:
    """Convert raw validation tests to mock result."""
    if not raw:
        return MockResult([], ["check_type", "expected", "result", "ran", "query"])
    output = []
    for row in raw:
        if row.raw_query and generator and not row.generated_query:
            try:
                row.generated_query = generator.compile_statement(row.raw_query)
            except Exception as e:
                row.generated_query = f"Error generating query: {e}"
        output.append(
            {
                "check_type": row.check_type.value,
                "expected": row.expected,
                "result": str(row.result) if row.result else None,
                "ran": row.ran,
                "query": row.generated_query if row.generated_query else "",
            }
        )
    return MockResult(output, ["check_type", "expected", "result", "ran", "query"])


def handle_processed_validate_statement(
    query: ProcessedValidateStatement, dialect: BaseDialect, validate_environment_func
) -> Optional[MockResult]:
    """Handle processed validate statements."""
    results = validate_environment_func(query.scope, query.targets)
    return raw_validation_to_result(results, dialect)


def handle_show_statement_outputs(
    statement: ProcessedShowStatement,
    compiled_statements: list[str],
    environment: Environment,
    dialect: BaseDialect,
) -> list[MockResult]:
    """Handle show statement outputs without execution."""
    output = []
    for x in statement.output_values:
        if isinstance(x, ProcessedStaticValueOutput):
            output.append(generate_result_set(statement.output_columns, x.values))
        elif compiled_statements:

            output.append(
                generate_result_set(
                    statement.output_columns,
                    compiled_statements,
                )
            )
        elif isinstance(x, ProcessedValidateStatement):
            from trilogy.core.validation.environment import validate_environment

            raw = validate_environment(environment, x.scope, x.targets)
            results = raw_validation_to_result(raw, dialect)
            if results:
                output.append(results)
        else:
            raise NotImplementedError(f"Cannot show type {type(x)} in show statement")
    return output
