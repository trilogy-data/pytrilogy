from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import ComparisonOperator
from trilogy.authoring import (
    Concept,
    Datasource,
    ConceptRef,
    Function,
    DataType,
    TraitDataType,
)
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
from trilogy.utility import unique


def type_check(
    input: Any, expected_type: DataType | TraitDataType, nullable: bool = True
) -> bool:
    if input is None and nullable:
        return True
    target_type = expected_type
    while isinstance(target_type, TraitDataType):
        target_type = target_type.data_type
    if target_type == DataType.STRING:
        return isinstance(input, str)
    if target_type == DataType.INTEGER:
        return isinstance(input, int)
    if target_type == DataType.FLOAT:
        return isinstance(input, float) or isinstance(input, int)
    if target_type == DataType.BOOL:
        return isinstance(input, bool)
    if target_type == DataType.DATE:
        return isinstance(input, str)  # TODO: improve date handling
    if target_type == DataType.DATETIME:
        return isinstance(input, str)  # TODO: improve datetime handling
    if target_type == DataType.ARRAY:
        return isinstance(input, list)
    if target_type == DataType.MAP:
        return isinstance(input, dict)
    return False


def validate_datasource(
    datasource: BuildDatasource, build_env: BuildEnvironment, exec: Executor
) -> list[DatasourceModelValidationError]:
    exceptions = []
    # we might have merged concepts, where both wil lmap out to the same
    unique_outputs = unique(
        [build_env.concepts[col.concept.address] for col in datasource.columns],
        "address",
    )
    type_query = easy_query(
        concepts=unique_outputs,
        datasource=datasource,
        env=exec.environment,
        limit=100,
    )
    type_sql = exec.generate_sql(type_query)[-1]
    try:
        rows = exec.execute_raw_sql(type_sql).fetchall()
    except Exception as e:
        exceptions.append(
            DatasourceModelValidationError(
                f"Datasource {datasource.name} failed validation. Error executing type query: {e}"
            )
        )
        return exceptions

    failures: list[tuple[str, Any, DataType | TraitDataType, bool]] = []
    cols_with_error = set()
    for row in rows:
        for col in datasource.columns:

            actual_address = build_env.concepts[col.concept.address].safe_address
            if actual_address in cols_with_error:
                continue
            rval = row[actual_address]
            passed = type_check(
                rval, col.concept.datatype, col.is_nullable
            )
            if not passed:
                failures.append(
                    (
                        col.concept.address,
                        rval,
                        col.concept.datatype,
                        col.is_nullable,
                    )
                )
                cols_with_error.add(actual_address)

    def format_failure(failure):
        return f"Concept {failure[0]} value '{failure[1]}' does not conform to expected type {str(failure[2])} (nullable={failure[3]})"

    if failures:
        exceptions.append(
            DatasourceModelValidationError(
                f"Datasource {datasource.name} failed validation. Found rows that do not conform to types: {[format_failure(failure) for failure in failures]}"
            )
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
        exceptions.append(
            DatasourceModelValidationError(
                f"Datasource {datasource.name} failed validation. Found rows that do not conform to grain: {rows}"
            )
        )

    return exceptions
