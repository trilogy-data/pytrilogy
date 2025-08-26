from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import ComparisonOperator
from trilogy.authoring import Concept, Datasource, ConceptRef, Function, DataType
from trilogy.core.enums import Purpose, FunctionType
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildConditional,
    BuildComparison,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import (
    CTE,
    QueryDatasource,
)
from trilogy.core.statements.execute import CTE, ProcessedQuery
from trilogy.hooks import DebuggingHook
from trilogy.parsing.common import function_to_concept
from typing import Any

from trilogy.core.validation.common import easy_query
from trilogy.core.exceptions import DatasourceModelValidationError

def type_check(input: Any, expected_type: DataType, nullable: bool = True) -> bool:
    if input is None and nullable:
        return True
    if expected_type == DataType.STRING:
        return isinstance(input, str)
    if expected_type == DataType.INTEGER:
        return isinstance(input, int)
    if expected_type == DataType.FLOAT:
        return isinstance(input, float) or isinstance(input, int)
    if expected_type == DataType.BOOL:
        return isinstance(input, bool)
    if expected_type == DataType.DATE:
        return isinstance(input, str)  # TODO: improve date handling
    if expected_type == DataType.DATETIME:
        return isinstance(input, str)  # TODO: improve datetime handling
    if expected_type == DataType.ARRAY:
        return isinstance(input, list)
    if expected_type == DataType.MAP:
        return isinstance(input, dict)
    return False


def validate_datasource(
    datasource: BuildDatasource, build_env: BuildEnvironment, exec: Executor
):
    type_query = easy_query(
        concepts=[build_env.concepts[col.concept.address] for col in datasource.columns],
        datasource=datasource,
        env=exec.environment,
        limit=100,
    )
    type_sql = exec.generate_sql(type_query)[-1]

    rows = exec.execute_raw_sql(type_sql).fetchall()
    failures: list[tuple[Any, DataType, bool]] = []
    for row in rows:
        for col in datasource.columns:

            passed = type_check(
                row[col.concept.safe_address], col.concept.datatype, col.is_nullable
            )
            if not passed:
                failures.append(
                    (
                        row[col.concept.safe_address],
                        col.concept.datatype,
                        col.is_nullable,
                    )
                )

    def format_failure(failure):
        return f"Table value '{failure[0]}' does not conform to expected type {failure[1].name} (nullable={failure[2]})"

    if failures:
        raise DatasourceModelValidationError(
            f"Datasource {datasource.name} failed validation. Found rows that do not conform to types: {[format_failure(failure) for failure in failures]}"
        )

    query = easy_query(
        concepts=[build_env.concepts[name] for name in datasource.grain.components]
        + [build_env.concepts["grain_check"]],
        datasource=datasource,
        env=exec.environment,
        condition=BuildComparison(
            left=build_env.concepts["grain_check"],
            right=1,
            operator=ComparisonOperator.GT,
        ),
    )
    sql = exec.generate_sql(query)[-1]

    rows = exec.execute_raw_sql(sql).fetchmany(10)
    if rows:
        raise ValueError(
            f"Datasource {datasource.name} failed validation. Found rows that do not conform to grain: {rows}"
        )
