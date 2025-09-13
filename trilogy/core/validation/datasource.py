from datetime import date, datetime
from decimal import Decimal
from typing import Any

from trilogy import Environment, Executor
from trilogy.authoring import (
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
    arg_to_datatype,
)
from trilogy.core.enums import ComparisonOperator, Modifier
from trilogy.core.exceptions import (
    DatasourceColumnBindingData,
    DatasourceColumnBindingError,
    DatasourceModelValidationError,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildDatasource,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.validation.common import ExpectationType, ValidationTest, easy_query
from trilogy.utility import unique


def type_check(
    input: Any,
    expected_type: (
        DataType | ArrayType | StructType | MapType | NumericType | TraitDataType
    ),
    nullable: bool = True,
) -> bool:
    if input is None and nullable:
        return True

    target_type = expected_type
    while isinstance(target_type, TraitDataType):
        return type_check(input, target_type.data_type, nullable)

    if target_type == DataType.STRING:
        return isinstance(input, str)
    if target_type == DataType.INTEGER:
        return isinstance(input, int)
    if target_type == DataType.BIGINT:
        return isinstance(input, int)  # or check for larger int if needed
    if target_type == DataType.FLOAT or isinstance(target_type, NumericType):
        return (
            isinstance(input, float)
            or isinstance(input, int)
            or isinstance(input, Decimal)
        )
    if target_type == DataType.NUMBER:
        return isinstance(input, (int, float, Decimal))
    if target_type == DataType.NUMERIC:
        return isinstance(input, (int, float, Decimal))
    if target_type == DataType.BOOL:
        return isinstance(input, bool)
    if target_type == DataType.DATE:
        return isinstance(input, date) and not isinstance(input, datetime)
    if target_type == DataType.DATETIME:
        return isinstance(input, datetime)
    if target_type == DataType.TIMESTAMP:
        return isinstance(input, datetime)  # or timestamp type if you have one
    if target_type == DataType.UNIX_SECONDS:
        return isinstance(input, (int, float))  # Unix timestamps are numeric
    if target_type == DataType.DATE_PART:
        return isinstance(
            input, str
        )  # assuming date parts are strings like "year", "month"
    if target_type == DataType.ARRAY or isinstance(target_type, ArrayType):
        return isinstance(input, list)
    if target_type == DataType.MAP or isinstance(target_type, MapType):
        return isinstance(input, dict)
    if target_type == DataType.STRUCT or isinstance(target_type, StructType):
        return isinstance(input, dict)
    if target_type == DataType.NULL:
        return input is None
    if target_type == DataType.UNKNOWN:
        return True
    return False


def validate_datasource(
    datasource: BuildDatasource,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None = None,
    fix: bool = False,
) -> list[ValidationTest]:
    results: list[ValidationTest] = []
    # we might have merged concepts, where both will map out to the same
    unique_outputs = unique(
        [build_env.concepts[col.concept.address] for col in datasource.columns],
        "address",
    )
    type_query = easy_query(
        concepts=unique_outputs,
        datasource=datasource,
        env=env,
        limit=100,
    )

    rows = []
    if exec:
        type_sql = exec.generate_sql(type_query)[-1]
        try:
            rows = exec.execute_raw_sql(type_sql).fetchall()
        except Exception as e:
            results.append(
                ValidationTest(
                    raw_query=type_query,
                    generated_query=type_sql,
                    check_type=ExpectationType.LOGICAL,
                    expected="valid_sql",
                    result=DatasourceModelValidationError(
                        f"Datasource {datasource.name} failed validation. Error executing type query {type_sql}: {e}"
                    ),
                    ran=True,
                )
            )
            return results
    else:

        results.append(
            ValidationTest(
                raw_query=type_query,
                check_type=ExpectationType.LOGICAL,
                expected="datatype_match",
                result=None,
                ran=False,
            )
        )
        return results
    failures: list[DatasourceColumnBindingData] = []
    cols_with_error = set()
    for row in rows:
        for col in datasource.columns:
            actual_address = build_env.concepts[col.concept.address].safe_address
            if actual_address in cols_with_error:
                continue
            rval = row[actual_address]
            passed = type_check(rval, col.concept.datatype, col.is_nullable)
            if not passed:
                value_type = (
                    arg_to_datatype(rval) if rval is not None else col.concept.datatype
                )
                traits = None
                if isinstance(col.concept.datatype, TraitDataType):
                    traits = col.concept.datatype.traits
                if traits and not isinstance(value_type, TraitDataType):
                    value_type = TraitDataType(type=value_type, traits=traits)
                failures.append(
                    DatasourceColumnBindingData(
                        address=col.concept.address,
                        value=rval,
                        value_type=value_type,
                        value_modifiers=[Modifier.NULLABLE] if rval is None else [],
                        actual_type=col.concept.datatype,
                        actual_modifiers=col.concept.modifiers,
                    )
                )
                cols_with_error.add(actual_address)

    if failures:
        results.append(
            ValidationTest(
                check_type=ExpectationType.LOGICAL,
                expected="datatype_match",
                ran=True,
                result=DatasourceColumnBindingError(
                    address=datasource.identifier, errors=failures
                ),
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
    if not exec:
        results.append(
            ValidationTest(
                raw_query=query,
                check_type=ExpectationType.ROWCOUNT,
                expected="0",
                result=None,
                ran=False,
            )
        )

    else:
        sql = exec.generate_sql(query)[-1]

        rows = exec.execute_raw_sql(sql).fetchmany(10)
        if rows:
            results.append(
                ValidationTest(
                    raw_query=query,
                    generated_query=sql,
                    check_type=ExpectationType.ROWCOUNT,
                    expected="0",
                    result=DatasourceModelValidationError(
                        f"Datasource {datasource.name} failed validation. Found rows that do not conform to grain: {rows}"
                    ),
                    ran=True,
                )
            )

    return results
