from collections.abc import Sequence
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
from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    Modifier,
    Purpose,
)
from trilogy.core.exceptions import (
    DatasourceColumnBindingData,
    DatasourceColumnBindingError,
    DatasourceGrainValidationError,
    DatasourceModelValidationError,
    render_datatype,
)
from trilogy.core.models.build import (
    BuildBetween,
    BuildComparison,
    BuildConcept,
    BuildConceptArgs,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildParenthetical,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    EnumType,
    TupleWrapper,
    ValidatedType,
)
from trilogy.core.validation.common import (
    ExpectationType,
    ValidationTest,
    easy_query,
    grain_check_address,
)
from trilogy.utility import unique

# how many violating rows a failed check reports before it just says "at least N"
SAMPLE_LIMIT = 10


def row_to_dict(row: Any, columns: Sequence[str]) -> dict[str, Any]:
    """Pair a result row with the column names its own result reported.

    Position is the only accessor every engine shares. `_mapping` is
    SQLAlchemy-only, and the namedtuple rows built by `trilogy.dialect.results`
    renumber any field name that is not a valid identifier — so neither the
    mapping nor the field names can be read off the row itself.
    """
    return dict(zip(columns, row))


def describe_violation_row(
    row: Any,
    columns: Sequence[str],
    concept: BuildConcept,
    keys: list[BuildConcept],
) -> str:
    """``genus.name='genus' -> genus.image='image_url'`` — lead with the grain
    keys so the offending row can actually be located in the source table."""
    values = row_to_dict(row, columns)
    offending = f"{concept.address}={values.get(concept.safe_address)!r}"
    rendered_keys = ", ".join(
        f"{key.address}={values[key.safe_address]!r}"
        for key in keys
        if key.safe_address in values
    )
    return f"{rendered_keys} -> {offending}" if rendered_keys else offending


def validate_unique_properties(
    datasource: BuildDatasource,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None,
) -> list[ValidationTest]:
    results: list[ValidationTest] = []
    output_addresses = {concept.address for concept in datasource.concepts}
    unique_properties = [
        concept
        for concept in datasource.concepts
        if concept.purpose is Purpose.UNIQUE_PROPERTY and concept.keys
    ]
    for concept in unique_properties:
        for key_address in sorted(concept.keys or ()):
            if key_address not in output_addresses:
                continue
            key = build_env.concepts[key_address]
            key_count = build_env.concepts.get(grain_check_address(key))
            if key_count is None:
                continue
            query = easy_query(
                concepts=[concept, key_count],
                datasource=datasource,
                env=env,
                condition=BuildComparison(
                    left=key_count,
                    right=1,
                    operator=ComparisonOperator.GT,
                ),
                grain=BuildGrain(components={concept.address}),
                limit=10,
            )
            if exec is None:
                results.append(
                    ValidationTest(
                        raw_query=query,
                        check_type=ExpectationType.ROWCOUNT,
                        expected="0",
                        result=None,
                        ran=False,
                    )
                )
                continue
            sql = exec.generate_sql(query)[-1]
            result = exec.execute_raw_sql(sql)
            columns = list(result.keys())
            rows = result.fetchmany(10)
            error = None
            if rows:
                error = DatasourceModelValidationError(
                    f"Datasource {datasource.name} failed validation. Unique "
                    f"property {concept.address} maps to multiple "
                    f"{key.address} values: "
                    f"{[row_to_dict(row, columns) for row in rows]}"
                )
            results.append(
                ValidationTest(
                    raw_query=query,
                    generated_query=sql,
                    check_type=ExpectationType.ROWCOUNT,
                    expected="0",
                    result=error,
                    ran=True,
                )
            )
    return results


def domain_violation_condition(
    concept: BuildConcept,
) -> BuildConditional | None:
    """A SQL-side predicate matching non-null rows whose value violates the
    concept's declared domain (ValidatedType ranges/regex or EnumType
    membership); None when the concept declares no checkable domain."""
    dt = concept.datatype
    while isinstance(dt, TraitDataType):
        dt = dt.type
    violation: BuildComparison | BuildConditional | BuildParenthetical | None = None
    if isinstance(dt, EnumType):
        if not dt.values:
            return None
        violation = BuildComparison(
            left=concept,
            operator=ComparisonOperator.NOT_IN,
            right=TupleWrapper(list(dt.values), type=dt.type),
        )
    elif isinstance(dt, ValidatedType):
        if dt.pattern is not None:
            # anchored to mirror the Python-side re.fullmatch semantics
            regex = BuildFunction(
                operator=FunctionType.REGEXP_CONTAINS,
                arguments=[concept, f"^(?:{dt.pattern})$"],
                output_data_type=DataType.BOOL,
                output_purpose=Purpose.PROPERTY,
                arg_count=2,
            )
            violation = BuildComparison(
                left=regex, operator=ComparisonOperator.EQ, right=False
            )
        elif dt.ranges:
            per_range: list[BuildComparison | BuildParenthetical] = []
            for r in dt.ranges:
                parts = []
                if r.min is not None:
                    parts.append(
                        BuildComparison(
                            left=concept, operator=ComparisonOperator.LT, right=r.min
                        )
                    )
                if r.max is not None:
                    parts.append(
                        BuildComparison(
                            left=concept, operator=ComparisonOperator.GT, right=r.max
                        )
                    )
                if len(parts) == 2:
                    per_range.append(
                        BuildParenthetical(
                            content=BuildConditional(
                                left=parts[0],
                                right=parts[1],
                                operator=BooleanOperator.OR,
                            )
                        )
                    )
                elif parts:
                    per_range.append(parts[0])
            if not per_range:
                return None
            violation = per_range[0]
            for nxt in per_range[1:]:
                violation = BuildConditional(
                    left=violation, right=nxt, operator=BooleanOperator.AND
                )
    if violation is None:
        return None
    return BuildConditional(
        left=BuildComparison(
            left=concept, operator=ComparisonOperator.IS_NOT, right=MagicConstants.NULL
        ),
        right=(
            violation
            if isinstance(violation, (BuildComparison, BuildParenthetical))
            else BuildParenthetical(content=violation)
        ),
        operator=BooleanOperator.AND,
    )


def validate_declared_domains(
    datasource: BuildDatasource,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None,
) -> list[ValidationTest]:
    """Full-table SQL-side domain checks: unlike the sampled type checks, these
    scan every row of the datasource for values outside a declared
    ValidatedType range/regex or EnumType membership."""
    results: list[ValidationTest] = []
    seen: set[str] = set()
    for col in datasource.columns:
        concept = build_env.concepts[col.concept.address]
        if concept.address in seen:
            continue
        seen.add(concept.address)
        condition = domain_violation_condition(concept)
        if condition is None:
            continue
        # select the datasource grain alongside the offending value so the error
        # can point at a locatable row rather than a bare orphan value
        keys = [
            build_env.concepts[address]
            for address in sorted(datasource.grain.components)
            if address != concept.address and address in build_env.concepts
        ]
        selected = [concept, *keys]
        query = easy_query(
            concepts=selected,
            datasource=datasource,
            env=env,
            condition=condition,
            grain=BuildGrain(components={c.address for c in selected}),
            limit=SAMPLE_LIMIT,
        )
        if exec is None:
            results.append(
                ValidationTest(
                    raw_query=query,
                    check_type=ExpectationType.ROWCOUNT,
                    expected="0",
                    result=None,
                    ran=False,
                )
            )
            continue
        sql = exec.generate_sql(query)[-1]
        result = exec.execute_raw_sql(sql)
        columns = list(result.keys())
        rows = result.fetchmany(SAMPLE_LIMIT)
        error = None
        if rows:
            counted = (
                f"{len(rows)} row(s)"
                if len(rows) < SAMPLE_LIMIT
                else f"at least {SAMPLE_LIMIT} rows"
            )
            samples = "\n".join(
                f"  {describe_violation_row(r, columns, concept, keys)}" for r in rows
            )
            error = DatasourceModelValidationError(
                f"Datasource {datasource.name} ({datasource.safe_location}) failed "
                f"validation. {counted} violate declared domain "
                f"{render_datatype(concept.datatype)} for {concept.address}. "
                f"Either fix the source data or widen the declared type.\n{samples}"
            )
        results.append(
            ValidationTest(
                raw_query=query,
                generated_query=sql,
                check_type=ExpectationType.ROWCOUNT,
                expected="0",
                result=error,
                ran=True,
            )
        )
    return results


def containment_violation_condition(
    conditional: BuildComparison | BuildConditional | BuildParenthetical | BuildBetween,
) -> BuildComparison:
    """Rows that do not provably satisfy ``conditional``.

    ``(cond is not distinct from True) = False`` rather than a negation: there
    is no generic NOT, and a plain ``cond = False`` would evaluate to NULL for
    a NULL-keyed row and let it escape. A row whose key is NULL is outside the
    claimed slice just as much as one keyed to another partition.
    """
    return BuildComparison(
        left=BuildFunction(
            operator=FunctionType.IS_NOT_DISTINCT,
            # the claim is one operand: `is not distinct from` binds tighter
            # than the `and` joining its atoms.
            arguments=[BuildParenthetical(content=conditional), True],
            output_data_type=DataType.BOOL,
            output_purpose=Purpose.PROPERTY,
            arg_count=2,
        ),
        operator=ComparisonOperator.EQ,
        right=False,
    )


def validate_complete_where_containment(
    datasource: BuildDatasource,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None,
) -> list[ValidationTest]:
    """A `complete where` is a containment claim the planner trusts and cannot
    enforce — it may name a column the source has no way to filter on, so query
    generation can only take it at its word and elide predicates it implies.
    Nothing else ever checks it, so a source that returns rows outside its own
    claim leaks them into every consumer with no error. This is the check that
    establishes the claim is true.

    Skipped, not failed, when the claim isn't evaluable against the datasource
    alone (a column it doesn't expose, or an existence subselect) — that is
    precisely the case generation can't inject either.
    """
    non_partial_for = datasource.non_partial_for
    if non_partial_for is None:
        return []
    conditional = non_partial_for.conditional
    if not isinstance(conditional, BuildConceptArgs):
        return []
    if any(arg for group in conditional.existence_arguments for arg in group):
        return []
    output_addresses = {concept.address for concept in datasource.concepts}
    claim_concepts = unique(list(conditional.row_arguments), "address")
    if not claim_concepts:
        return []
    if not all(c.address in output_addresses for c in claim_concepts):
        return []

    keys = [
        build_env.concepts[address]
        for address in sorted(datasource.grain.components)
        if address in build_env.concepts
    ]
    selected = unique(claim_concepts + keys, "address")
    query = easy_query(
        concepts=selected,
        datasource=datasource,
        env=env,
        condition=containment_violation_condition(conditional),
        grain=BuildGrain(components={c.address for c in selected}),
        limit=SAMPLE_LIMIT,
    )
    if exec is None:
        return [
            ValidationTest(
                raw_query=query,
                check_type=ExpectationType.ROWCOUNT,
                expected="0",
                result=None,
                ran=False,
            )
        ]
    sql = exec.generate_sql(query)[-1]
    result = exec.execute_raw_sql(sql)
    columns = list(result.keys())
    rows = result.fetchmany(SAMPLE_LIMIT)
    error = None
    if rows:
        counted = (
            f"{len(rows)} row(s)"
            if len(rows) < SAMPLE_LIMIT
            else f"at least {SAMPLE_LIMIT} rows"
        )
        samples = "\n".join(f"  {row_to_dict(r, columns)}" for r in rows)
        error = DatasourceModelValidationError(
            f"Datasource {datasource.name} ({datasource.safe_location}) failed "
            f"validation. {counted} fall outside its `complete where "
            f"{conditional}` claim. The planner trusts that claim and drops "
            "predicates it implies, so these rows leak into consumers "
            "unfiltered. Either filter the source (a `where` clause on the "
            "datasource, or the query/script behind it) or narrow the "
            f"claim.\n{samples}"
        )
    return [
        ValidationTest(
            raw_query=query,
            generated_query=sql,
            check_type=ExpectationType.ROWCOUNT,
            expected="0",
            result=error,
            ran=True,
        )
    ]


def type_check(
    input: Any,
    expected_type: CONCRETE_TYPES,
    nullable: bool = True,
) -> bool:
    if input is None and nullable:
        return True

    target_type = expected_type
    while isinstance(target_type, TraitDataType):
        return type_check(input, target_type.data_type, nullable)

    if isinstance(target_type, EnumType):
        return (
            type_check(input, target_type.type, nullable)
            and input in target_type.values
        )

    if isinstance(target_type, ValidatedType):
        return type_check(
            input, target_type.type, nullable
        ) and target_type.check_value(input)

    if target_type == DataType.STRING:
        return isinstance(input, str)
    if target_type == DataType.BYTES:
        return isinstance(input, (bytes, bytearray, memoryview))
    if target_type == DataType.INTEGER:
        return isinstance(input, int)
    if target_type == DataType.BIGINT:
        return isinstance(input, int)  # or check for larger int if needed
    if target_type in (DataType.FLOAT, DataType.DOUBLE) or isinstance(
        target_type, NumericType
    ):
        return isinstance(input, (float, int, Decimal))
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
    if target_type == DataType.GEOGRAPHY:
        # Unsafe compatibility shim: some DuckDB geography values currently surface
        # as raw bytes, but not all bytes payloads are valid geometries.
        return isinstance(input, (bytes, bytearray, memoryview))
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
    return target_type == DataType.UNKNOWN


def inferred_type_check(
    inferred_type: CONCRETE_TYPES,
    expected_type: CONCRETE_TYPES,
) -> bool:
    while isinstance(inferred_type, TraitDataType):
        inferred_type = inferred_type.data_type

    target_type = expected_type
    while isinstance(target_type, TraitDataType):
        target_type = target_type.data_type

    if isinstance(inferred_type, EnumType) or isinstance(target_type, EnumType):
        return (
            isinstance(inferred_type, EnumType)
            and isinstance(target_type, EnumType)
            and inferred_type == target_type
        )

    if isinstance(inferred_type, ValidatedType) or isinstance(
        target_type, ValidatedType
    ):
        return (
            isinstance(inferred_type, ValidatedType)
            and isinstance(target_type, ValidatedType)
            and inferred_type == target_type
        )

    return inferred_type == target_type


def validate_datasource(
    datasource: BuildDatasource,
    env: Environment,
    build_env: BuildEnvironment,
    exec: Executor | None = None,
    fix: bool = False,
) -> list[ValidationTest]:
    results: list[ValidationTest] = []
    datasource_output_addresses = {concept.address for concept in datasource.concepts}
    missing_grain_components = sorted(
        component
        for component in datasource.grain.components
        if component not in datasource_output_addresses
    )
    if missing_grain_components:
        results.append(
            ValidationTest(
                check_type=ExpectationType.LOGICAL,
                expected="grain_columns_present",
                ran=True,
                result=DatasourceGrainValidationError(
                    "Datasource"
                    f" {datasource.name} failed validation. Grain references"
                    " concepts not present in datasource output:"
                    f" {', '.join(missing_grain_components)}"
                ),
            )
        )
        return results

    validation_datasource = (
        exec.get_validation_cached_datasource(datasource) if exec else datasource
    )
    # we might have merged concepts, where both will map out to the same
    unique_outputs = unique(
        [
            build_env.concepts[col.concept.address]
            for col in validation_datasource.columns
        ],
        "address",
    )
    type_query = easy_query(
        concepts=unique_outputs,
        datasource=validation_datasource,
        env=env,
        limit=100,
    )

    rows = []
    type_columns: list[str] = []
    result_column_types: dict[str, CONCRETE_TYPES] = {}
    if exec:
        type_sql = exec.generate_sql(type_query)[-1]
        try:
            result = exec.execute_raw_sql(type_sql)
            result_column_types = (
                exec.generator.get_result_column_types_for_validation(result) or {}
            )
            type_columns = list(result.keys())
            rows = result.fetchall()
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
    refined_type_cache: dict[tuple[str, str, str], CONCRETE_TYPES] = {}
    for row in rows:
        values = row_to_dict(row, type_columns)
        for col in datasource.columns:
            actual_address = build_env.concepts[col.concept.address].safe_address
            if actual_address in cols_with_error:
                continue
            rval = values[actual_address]
            passed = type_check(rval, col.concept.datatype, col.is_nullable)
            value_type = None
            if not passed:
                value_type = (
                    arg_to_datatype(rval) if rval is not None else col.concept.datatype
                )
                if rval is not None and exec:
                    cache_key = (
                        actual_address,
                        str(value_type),
                        str(col.concept.datatype),
                    )
                    if cache_key not in refined_type_cache:
                        refined_type_cache[cache_key] = (
                            exec.generator.refine_runtime_value_type_for_validation(
                                exec,
                                rval,
                                value_type,
                                col.concept.datatype,
                                result_type=result_column_types.get(actual_address),
                            )
                        )
                    value_type = refined_type_cache[cache_key]
                    passed = inferred_type_check(value_type, col.concept.datatype)
            if not passed:
                assert value_type is not None
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
    results += validate_unique_properties(
        validation_datasource,
        env,
        build_env,
        exec,
    )
    results += validate_declared_domains(
        validation_datasource,
        env,
        build_env,
        exec,
    )
    results += validate_complete_where_containment(
        validation_datasource,
        env,
        build_env,
        exec,
    )
    if not datasource.grain.components:
        return results

    # grain validation section
    query = easy_query(
        concepts=[
            build_env.concepts[name] for name in validation_datasource.grain.components
        ]
        + [build_env.concepts["grain_check"]],
        datasource=validation_datasource,
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

        grain_result = exec.execute_raw_sql(sql)
        grain_columns = list(grain_result.keys())
        rows = grain_result.fetchmany(10)
        if rows:
            results.append(
                ValidationTest(
                    raw_query=query,
                    generated_query=sql,
                    check_type=ExpectationType.ROWCOUNT,
                    expected="0",
                    result=DatasourceModelValidationError(
                        f"Datasource {datasource.name} failed validation. Found rows that do not conform to grain: {[row_to_dict(r, grain_columns) for r in rows]}"
                    ),
                    ran=True,
                )
            )

    return results
