from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Set

from trilogy.constants import MagicConstants

if TYPE_CHECKING:
    from trilogy.parsing.helpers import Meta
from trilogy.core.enums import (
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    InfiniteFunctionArgs,
    Purpose,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AggregateWrapper,
    CaseElse,
    CaseSimpleWhen,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    CustomType,
    FilterItem,
    Function,
    NavigationWindowItem,
    NumberingWindowItem,
    Parenthetical,
    RowsetItem,
    UndefinedConcept,
)
from trilogy.core.models.build import BuildConcept, BuildFunction
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
    arg_to_datatype,
    is_compatible_datatype,
    merge_datatypes,
)
from trilogy.core.models.environment import Environment

GENERIC_ARGS = Concept | ConceptRef | Function | str | int | float | date | datetime


CUSTOM_PLACEHOLDER = CustomType(
    name="__placeholder__", type=DataType.UNKNOWN, drop_on=[], add_on=[]
)


VALID_INPUT_ITEM = DataType | ArrayType | MapType
VALID_INPUTS_TYPE = Set[VALID_INPUT_ITEM] | List[Set[VALID_INPUT_ITEM]]


@dataclass
class FunctionConfig:
    arg_count: int = 1
    valid_inputs: VALID_INPUTS_TYPE | None = None
    output_purpose: Purpose | None = None
    output_type: (
        DataType | ArrayType | MapType | StructType | NumericType | TraitDataType | None
    ) = None
    output_type_function: Optional[Callable] = None


def get_unnest_output_type(args: list[Any]) -> CONCRETE_TYPES:
    output = arg_to_datatype(args[0])
    if isinstance(output, (ArrayType, MapType)):
        output = output.value_data_type
    else:
        output = DataType.STRING
    return output


def get_coalesce_output_type(args: list[Any]) -> CONCRETE_TYPES:
    non_null = [x for x in args if not x == MagicConstants.NULL]
    processed = [arg_to_datatype(x) for x in non_null if x]
    if not processed:
        return DataType.UNKNOWN
    # Bucket by base family so that traits + parameterized variants of the
    # same family (e.g. numeric(15,2)::usd, numeric::usd) collapse together.
    reps = _representative_types(processed)
    if len(reps) == 1:
        return reps[0]
    # Distinct families are fine when pairwise compatible (e.g. mixing FLOAT /
    # NUMERIC / INTEGER) — coalesce to their merged type, matching CASE and SQL
    # (`coalesce(sum(x)::float, 0::numeric)`). Only genuinely-incompatible
    # families (e.g. FLOAT vs STRING) are an error.
    for i, a in enumerate(reps):
        if any(not is_compatible_datatype(a, b) for b in reps[i + 1 :]):
            raise InvalidSyntaxException(
                f"All arguments to coalesce must be of compatible types, have "
                f"{set(arg_to_datatype(x) for x in args)} for {str(args)}"
            )
    return merge_datatypes(reps)


def get_transform_output_type(args: list[Any]) -> CONCRETE_TYPES:
    return ArrayType(type=arg_to_datatype(args[2]))


def get_index_output_type(
    args: list[Any],
) -> CONCRETE_TYPES:
    arg = args[0]
    datatype = arg_to_datatype(arg)
    if isinstance(datatype, ArrayType):
        return datatype.value_data_type
    elif isinstance(datatype, MapType):
        return datatype.value_data_type
    return datatype


def get_attr_datatype(
    args: list[Any],
) -> CONCRETE_TYPES:
    arg = args[0]
    lookup = args[1]
    datatype = arg_to_datatype(arg)
    if isinstance(datatype, StructType):
        return datatype.field_types[lookup]
    return datatype


def get_cast_output_type(
    args: list[Any],
) -> CONCRETE_TYPES:
    base = arg_to_datatype(args[0])
    if isinstance(base, TraitDataType):
        traits = base.traits
    else:
        traits = []
    if isinstance(args[1], TraitDataType):
        return TraitDataType(
            type=args[1].type, traits=list(set(traits + args[1].traits))
        )
    elif traits:
        return TraitDataType(type=args[1], traits=traits)
    return args[1]


def get_output_type_at_index(args, index: int):
    return arg_to_datatype(args[index])


LITERAL_CONSTANT_TYPES = (bool, int, float, str, bytes, Decimal, date, datetime)


def _is_literal_constant(expr: Any) -> bool:
    return isinstance(expr, LITERAL_CONSTANT_TYPES)


def _representative_types(types: list[CONCRETE_TYPES]) -> list[CONCRETE_TYPES]:
    """One representative per distinct base DataType, preferring richer
    (parameterized) types like NumericType over their bare DataType enum."""

    def _bucket(t: CONCRETE_TYPES) -> Any:
        # Unwrap traits — they are pure annotations and should bucket with
        # their underlying type. Then collapse parameterized wrappers
        # (NumericType, ArrayType, ...) down to their base DataType enum so
        # NumericType(15,2) and bare DataType.NUMERIC share a bucket.
        inner = t.type if isinstance(t, TraitDataType) else t
        return inner.data_type if not isinstance(inner, DataType) else inner

    by_base: dict[Any, CONCRETE_TYPES] = {}
    for t in types:
        key = _bucket(t)
        existing = by_base.get(key)
        if existing is None or (
            isinstance(existing, DataType) and not isinstance(t, DataType)
        ):
            by_base[key] = t
    return list(by_base.values())


def _resolve_case_output(branches: list[Any]) -> CONCRETE_TYPES:
    """Compute a CASE's output type.

    Non-constant branches stay strict (differing column types are a likely
    modeling error). A literal-constant branch whose type is compatible with
    the non-constant branch is widened into it, so e.g. ``ELSE 0.0`` inherits
    the ``NumericType`` of the ``THEN`` column instead of forcing a cast.
    """
    non_constant: list[CONCRETE_TYPES] = []
    constant: list[CONCRETE_TYPES] = []
    mapz: dict[str, CONCRETE_TYPES] = {}
    for branch in branches:
        dt = arg_to_datatype(branch.expr)
        mapz[str(branch.expr)] = dt
        if dt == DataType.NULL or dt.data_type == DataType.UNKNOWN:
            continue
        (constant if _is_literal_constant(branch.expr) else non_constant).append(dt)

    def _fail() -> None:
        seen = {t.data_type for t in non_constant + constant}
        raise InvalidSyntaxException(
            f"All case expressions must have the same output datatype, got {seen} from {mapz}"
        )

    nc = _representative_types(non_constant)
    if len(nc) > 1:
        _fail()

    if nc:
        target = nc[0]
        if any(not is_compatible_datatype(target, c) for c in constant):
            _fail()
        return merge_datatypes([target, *constant]) if constant else target

    cc = _representative_types(constant)
    if not cc:
        return DataType.UNKNOWN
    if len(cc) == 1:
        return cc[0]
    for i, a in enumerate(cc):
        if any(not is_compatible_datatype(a, b) for b in cc[i + 1 :]):
            _fail()
    return merge_datatypes(cc)


def validate_simple_case_output(args: list[Any]) -> CONCRETE_TYPES:
    return _resolve_case_output(args[1:])


def validate_case_output(args: list[Any]) -> CONCRETE_TYPES:
    return _resolve_case_output(args)


def create_struct_output(
    args: list[Any],
) -> StructType:
    zipped = dict(zip(args[1::2], args[::2]))
    types = [arg_to_datatype(x) for x in args[::2]]
    return StructType(fields=types, fields_map=zipped)


def get_date_part_output(args: list[Any]):
    target = args[1]
    if target == DatePart.YEAR:
        return TraitDataType(type=DataType.INTEGER, traits=["year"])
    elif target == DatePart.MONTH:
        return TraitDataType(type=DataType.INTEGER, traits=["month"])
    elif target == DatePart.DAY:
        return TraitDataType(type=DataType.INTEGER, traits=["day"])
    elif target == DatePart.HOUR:
        return TraitDataType(type=DataType.INTEGER, traits=["hour"])
    elif target == DatePart.MINUTE:
        return TraitDataType(type=DataType.INTEGER, traits=["minute"])
    elif target == DatePart.SECOND:
        return TraitDataType(type=DataType.INTEGER, traits=["second"])
    elif target == DatePart.WEEK:
        return TraitDataType(type=DataType.INTEGER, traits=["week"])
    elif target == DatePart.QUARTER:
        return TraitDataType(type=DataType.INTEGER, traits=["quarter"])
    elif target == DatePart.DAY_OF_WEEK:
        return TraitDataType(type=DataType.INTEGER, traits=["day_of_week"])
    else:
        raise InvalidSyntaxException(f"Date part not supported for {target}")


def get_date_trunc_output(
    args: list[Any],
):
    target: DatePart = args[1]
    if target == DatePart.YEAR:
        return DataType.DATE
    elif target == DatePart.MONTH:
        return DataType.DATE
    elif target == DatePart.DAY:
        return DataType.DATE
    elif target == DatePart.HOUR:
        return DataType.DATETIME
    elif target == DatePart.MINUTE:
        return DataType.DATETIME
    elif target == DatePart.SECOND:
        return DataType.DATETIME
    elif target == DatePart.WEEK:
        return DataType.DATE
    elif target == DatePart.QUARTER:
        return DataType.DATE
    else:
        raise InvalidSyntaxException(f"Date truncation not supported for {target}")


def get_map_key_type(arg):
    arg_datatype = arg_to_datatype(arg)
    if isinstance(arg_datatype, MapType):
        return ArrayType(type=arg_datatype.key_data_type)
    return ArrayType(type=DataType.STRING)


def get_map_value_type(arg):
    arg_datatype = arg_to_datatype(arg)
    if isinstance(arg_datatype, MapType):
        return ArrayType(type=arg_datatype.value_data_type)
    return ArrayType(type=DataType.STRING)


# Numeric argument types accepted by math / aggregate functions. SQL backends
# coerce freely across this family, so a function that accepts one accepts all.
NUMERIC_INPUT_TYPES: Set[VALID_INPUT_ITEM] = {
    DataType.INTEGER,
    DataType.BIGINT,
    DataType.FLOAT,
    DataType.DOUBLE,
    DataType.NUMBER,
    DataType.NUMERIC,
}


FUNCTION_REGISTRY: dict[FunctionType, FunctionConfig] = {
    FunctionType.ALIAS: FunctionConfig(
        arg_count=1,
    ),
    FunctionType.NOOP: FunctionConfig(
        arg_count=1,
    ),
    FunctionType.PARENTHETICAL: FunctionConfig(
        arg_count=1,
    ),
    FunctionType.UNNEST: FunctionConfig(
        valid_inputs={
            DataType.ARRAY,
        },
        output_purpose=Purpose.KEY,
        output_type_function=get_unnest_output_type,
        arg_count=1,
    ),
    FunctionType.DATE_SPINE: FunctionConfig(
        valid_inputs={
            DataType.DATE,
        },
        output_purpose=Purpose.KEY,
        output_type=DataType.DATE,
        arg_count=2,
    ),
    FunctionType.RECURSE_EDGE: FunctionConfig(
        arg_count=2,
    ),
    FunctionType.GROUP: FunctionConfig(
        arg_count=-1,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
    ),
    FunctionType.COUNT: FunctionConfig(
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.COUNT_DISTINCT: FunctionConfig(
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.GROUPING: FunctionConfig(
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.GROUPING_ID: FunctionConfig(
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=InfiniteFunctionArgs,
    ),
    # MAX/MIN are lexicographic over STRING and orderable for BOOL in every SQL
    # dialect we target (DuckDB, Postgres, SQLite, BigQuery, Snowflake, Trino,
    # MySQL, Redshift, SQL Server), so STRING/BOOL are valid inputs.
    FunctionType.MAX: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES
        | {
            DataType.DATE,
            DataType.DATETIME,
            DataType.TIMESTAMP,
            DataType.BOOL,
            DataType.STRING,
        },
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.MIN: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES
        | {
            DataType.DATE,
            DataType.DATETIME,
            DataType.TIMESTAMP,
            DataType.BOOL,
            DataType.STRING,
        },
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.SPLIT: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=ArrayType(type=DataType.STRING),
        arg_count=2,
    ),
    FunctionType.INDEX_ACCESS: FunctionConfig(
        valid_inputs=[
            {
                DataType.ARRAY,
            },
            {
                DataType.INTEGER,
            },
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=get_index_output_type,
        arg_count=2,
    ),
    FunctionType.MAP_KEYS: FunctionConfig(
        valid_inputs={
            DataType.MAP,
        },
        output_purpose=Purpose.PROPERTY,
        output_type_function=lambda args: get_map_key_type(args[0]),
        arg_count=1,
    ),
    FunctionType.MAP_VALUES: FunctionConfig(
        valid_inputs={
            DataType.MAP,
        },
        output_purpose=Purpose.PROPERTY,
        output_type_function=lambda args: get_map_value_type(args[0]),
        arg_count=1,
    ),
    FunctionType.GENERATE_ARRAY: FunctionConfig(
        valid_inputs={
            DataType.INTEGER,
            DataType.INTEGER,
            DataType.INTEGER,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=ArrayType(type=DataType.INTEGER),
        arg_count=3,
    ),
    FunctionType.ARRAY_DISTINCT: FunctionConfig(
        valid_inputs={
            DataType.ARRAY,
        },
        output_purpose=Purpose.PROPERTY,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
        arg_count=1,
    ),
    FunctionType.ARRAY_SORT: FunctionConfig(
        valid_inputs=[
            {DataType.ARRAY},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
        arg_count=2,
    ),
    FunctionType.ARRAY_TRANSFORM: FunctionConfig(
        valid_inputs=[
            {
                DataType.ARRAY,
            },
            {*DataType},
            {*DataType},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=get_transform_output_type,
        arg_count=3,
    ),
    FunctionType.ARRAY_FILTER: FunctionConfig(
        valid_inputs=[
            {
                DataType.ARRAY,
            },
            {*DataType},
            {*DataType},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=get_transform_output_type,
        arg_count=3,
    ),
    FunctionType.ARRAY_TO_STRING: FunctionConfig(
        valid_inputs=[
            {ArrayType(type=DataType.STRING)},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=2,
    ),
    FunctionType.ARRAY_SUM: FunctionConfig(
        valid_inputs={
            ArrayType(type=DataType.INTEGER),
            ArrayType(type=DataType.BIGINT),
            ArrayType(type=DataType.FLOAT),
            ArrayType(type=DataType.NUMBER),
            ArrayType(type=DataType.NUMERIC),
        },
        output_purpose=Purpose.PROPERTY,
        output_type_function=get_index_output_type,
        arg_count=1,
    ),
    FunctionType.MAP_ACCESS: FunctionConfig(
        valid_inputs=[
            {
                DataType.MAP,
            },
            {
                DataType.INTEGER,
                DataType.STRING,
            },
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=get_index_output_type,
        arg_count=2,
    ),
    FunctionType.ATTR_ACCESS: FunctionConfig(
        valid_inputs=[
            {DataType.STRUCT},
            {
                DataType.STRING,
            },
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=get_attr_datatype,
        arg_count=2,
    ),
    FunctionType.ABS: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.PROPERTY,
        arg_count=1,
    ),
    FunctionType.NULLIF: FunctionConfig(
        valid_inputs={*DataType},
        output_purpose=Purpose.PROPERTY,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
        arg_count=2,
    ),
    FunctionType.COALESCE: FunctionConfig(
        valid_inputs={*DataType},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=-1,
        output_type_function=get_coalesce_output_type,
    ),
    FunctionType.GREATEST: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES
        | {DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP, DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=-1,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
    ),
    FunctionType.LEAST: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES
        | {DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP, DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=-1,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
    ),
    FunctionType.CURRENT_DATE: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        output_type=DataType.DATE,
        arg_count=0,
    ),
    FunctionType.CURRENT_DATETIME: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        output_type=DataType.DATETIME,
        arg_count=0,
    ),
    FunctionType.CURRENT_TIMESTAMP: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        output_type=DataType.TIMESTAMP,
        arg_count=0,
    ),
    FunctionType.BOOL: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=1,
    ),
    FunctionType.STRPOS: FunctionConfig(
        valid_inputs=[
            {DataType.STRING},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=2,
    ),
    FunctionType.REPLACE: FunctionConfig(
        valid_inputs=[
            {DataType.STRING},
            {DataType.STRING},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=3,
    ),
    FunctionType.CONTAINS: FunctionConfig(
        valid_inputs=[
            {DataType.STRING},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=2,
    ),
    FunctionType.TRIM: FunctionConfig(
        valid_inputs=[
            {DataType.STRING},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=2,
    ),
    FunctionType.LTRIM: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=1,
    ),
    FunctionType.RTRIM: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=1,
    ),
    FunctionType.SUBSTRING: FunctionConfig(
        valid_inputs=[{DataType.STRING}, {DataType.INTEGER}, {DataType.INTEGER}],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=3,
    ),
    FunctionType.UNION: FunctionConfig(
        valid_inputs={*DataType},
        output_purpose=Purpose.KEY,
        arg_count=-1,
    ),
    FunctionType.UPPER: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=1,
    ),
    FunctionType.LOWER: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=1,
    ),
    FunctionType.REGEXP_CONTAINS: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=2,
    ),
    FunctionType.REGEXP_EXTRACT: FunctionConfig(
        valid_inputs=[{DataType.STRING}, {DataType.STRING}, {DataType.INTEGER}],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=3,
    ),
    FunctionType.REGEXP_REPLACE: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=3,
    ),
    FunctionType.DATE: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.DATE,
        arg_count=1,
    ),
    FunctionType.DATE_TRUNCATE: FunctionConfig(
        valid_inputs=[
            {
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
            },
            {DataType.DATE_PART},
        ],
        output_purpose=Purpose.PROPERTY,
        # output_type=DataType.DATE,
        output_type_function=get_date_trunc_output,
        arg_count=2,
    ),
    FunctionType.DATE_PART: FunctionConfig(
        valid_inputs=[
            {
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
            },
            {DataType.DATE_PART},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=get_date_part_output,
        arg_count=2,
    ),
    FunctionType.DATE_ADD: FunctionConfig(
        valid_inputs=[
            {
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
                DataType.STRING,
            },
            {DataType.DATE_PART},
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.DATE,
        arg_count=3,
    ),
    FunctionType.DATE_SUB: FunctionConfig(
        valid_inputs=[
            {
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
                DataType.STRING,
            },
            {DataType.DATE_PART},
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.DATE,
        arg_count=3,
    ),
    FunctionType.DATE_DIFF: FunctionConfig(
        valid_inputs=[
            {
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
                DataType.STRING,
            },
            {
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
                DataType.STRING,
            },
            {DataType.DATE_PART},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=3,
    ),
    FunctionType.DATETIME: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.DATETIME,
        arg_count=1,
    ),
    FunctionType.TIMESTAMP: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.TIMESTAMP,
        arg_count=1,
    ),
    FunctionType.SECOND: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["second"]),
        arg_count=1,
    ),
    FunctionType.MINUTE: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["minute"]),
        arg_count=1,
    ),
    FunctionType.HOUR: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["hour"]),
        arg_count=1,
    ),
    FunctionType.DAY: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["day"]),
        arg_count=1,
    ),
    FunctionType.DAY_NAME: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            # DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.STRING, traits=["day_name"]),
        arg_count=1,
    ),
    FunctionType.WEEK: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["week"]),
        arg_count=1,
    ),
    FunctionType.MONTH: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["month"]),
        arg_count=1,
    ),
    FunctionType.MONTH_NAME: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            # DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.STRING, traits=["month_name"]),
        arg_count=1,
    ),
    FunctionType.FORMAT_TIME: FunctionConfig(
        valid_inputs=[
            {
                DataType.DATE,
                DataType.TIMESTAMP,
                DataType.DATETIME,
            },
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=2,
    ),
    FunctionType.PARSE_TIME: FunctionConfig(
        valid_inputs=[
            {DataType.STRING},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.DATETIME,
        arg_count=2,
    ),
    FunctionType.QUARTER: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["quarter"]),
        arg_count=1,
    ),
    FunctionType.YEAR: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["year"]),
        arg_count=1,
    ),
    FunctionType.DAY_OF_WEEK: FunctionConfig(
        valid_inputs={
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.DATETIME,
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=TraitDataType(type=DataType.INTEGER, traits=["day_of_week"]),
        arg_count=1,
    ),
    FunctionType.ADD: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.SUBTRACT: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.MULTIPLY: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.POWER: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.PROPERTY,
        arg_count=2,
    ),
    FunctionType.DIVIDE: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.MOD: FunctionConfig(
        valid_inputs=[
            NUMERIC_INPUT_TYPES,
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=2,
    ),
    FunctionType.SQRT: FunctionConfig(
        valid_inputs=[
            NUMERIC_INPUT_TYPES,
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.LOG: FunctionConfig(
        valid_inputs=[
            NUMERIC_INPUT_TYPES,
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.FLOAT,
        arg_count=2,
    ),
    FunctionType.RANDOM: FunctionConfig(
        valid_inputs=[],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.FLOAT,
        arg_count=1,
    ),
    FunctionType.ROUND: FunctionConfig(
        valid_inputs=[
            NUMERIC_INPUT_TYPES,
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
        arg_count=2,
    ),
    FunctionType.FLOOR: FunctionConfig(
        valid_inputs=[
            NUMERIC_INPUT_TYPES,
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.CEIL: FunctionConfig(
        valid_inputs=[
            NUMERIC_INPUT_TYPES,
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.CUSTOM: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.SIMPLE_CASE: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        output_type_function=validate_simple_case_output,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.CASE: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        output_type_function=validate_case_output,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.CAST: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        arg_count=2,
        output_type_function=get_cast_output_type,
    ),
    FunctionType.CONCAT: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.CONCAT_STRICT: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.CONCAT_WS: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.CONSTANT: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        arg_count=1,
    ),
    FunctionType.TYPED_CONSTANT: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        output_type_function=get_cast_output_type,
        arg_count=2,
    ),
    FunctionType.IS_NULL: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=1,
    ),
    FunctionType.IS_NOT_DISTINCT: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=2,
    ),
    FunctionType.STRUCT: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
        output_type_function=create_struct_output,
    ),
    FunctionType.ARRAY: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
        output_type=ArrayType(type=DataType.STRING),
    ),
    FunctionType.LENGTH: FunctionConfig(
        valid_inputs={DataType.STRING, DataType.ARRAY, DataType.MAP},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.SUM: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES | {DataType.BOOL},
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.ARRAY_AGG: FunctionConfig(
        valid_inputs={*DataType},
        output_purpose=Purpose.METRIC,
        output_type_function=lambda args: ArrayType(
            type=merge_datatypes([arg_to_datatype(x) for x in args])
        ),
        arg_count=1,
    ),
    FunctionType.ANY: FunctionConfig(
        valid_inputs={*DataType},
        output_purpose=Purpose.PROPERTY,
        arg_count=1,
    ),
    FunctionType.BOOL_AND: FunctionConfig(
        valid_inputs={DataType.BOOL},
        output_purpose=Purpose.METRIC,
        output_type=DataType.BOOL,
        arg_count=1,
    ),
    FunctionType.BOOL_OR: FunctionConfig(
        valid_inputs={DataType.BOOL},
        output_purpose=Purpose.METRIC,
        output_type=DataType.BOOL,
        arg_count=1,
    ),
    FunctionType.AVG: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.STDDEV: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.METRIC,
        output_type=DataType.FLOAT,
        arg_count=1,
    ),
    FunctionType.VARIANCE: FunctionConfig(
        valid_inputs=NUMERIC_INPUT_TYPES,
        output_purpose=Purpose.METRIC,
        output_type=DataType.FLOAT,
        arg_count=1,
    ),
    FunctionType.UNIX_TO_TIMESTAMP: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.BIGINT},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.TIMESTAMP,
        arg_count=1,
    ),
    FunctionType.HASH: FunctionConfig(
        valid_inputs={
            DataType.STRING,
        },
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=2,
    ),
    FunctionType.HEX: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.STRING,
        arg_count=1,
    ),
    FunctionType.GEO_POINT: FunctionConfig(
        valid_inputs={DataType.NUMERIC},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.GEOGRAPHY,
        arg_count=2,
    ),
    FunctionType.GEO_FROM_TEXT: FunctionConfig(
        valid_inputs={DataType.STRING, DataType.BYTES},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.GEOGRAPHY,
        arg_count=1,
    ),
    FunctionType.GEO_DISTANCE: FunctionConfig(
        valid_inputs={DataType.GEOGRAPHY},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.NUMERIC,
        arg_count=2,
    ),
    FunctionType.GEO_X: FunctionConfig(
        valid_inputs={DataType.GEOGRAPHY},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.NUMERIC,
        arg_count=1,
    ),
    FunctionType.GEO_Y: FunctionConfig(
        valid_inputs={DataType.GEOGRAPHY},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.NUMERIC,
        arg_count=1,
    ),
    FunctionType.GEO_CENTROID: FunctionConfig(
        valid_inputs={DataType.GEOGRAPHY},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.GEOGRAPHY,
        arg_count=1,
    ),
    FunctionType.GEO_TRANSFORM: FunctionConfig(
        valid_inputs=[
            {DataType.GEOGRAPHY},
            NUMERIC_INPUT_TYPES,
            NUMERIC_INPUT_TYPES,
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.GEOGRAPHY,
        arg_count=3,
    ),
}

EXCLUDED_FUNCTIONS = {
    FunctionType.CUSTOM,
    # Temporary
    FunctionType.DATE_LITERAL,
    FunctionType.DATETIME_LITERAL,
    FunctionType.ARRAY,
    # constructed directly by the parser for composite membership, never via create_function
    FunctionType.ROW_TUPLE,
}

for k in FunctionType.__members__.values():
    if k not in FUNCTION_REGISTRY and k not in EXCLUDED_FUNCTIONS:
        raise InvalidSyntaxException(
            f"Function enum value {k} not in creation registry"
        )

# scalar functions that can return non-null from nullable inputs; a BASIC
# derivation through anything else propagates its arguments' nullability
NULL_SUPPRESSING_FUNCTIONS = {
    FunctionType.COALESCE,
    FunctionType.CASE,
    FunctionType.SIMPLE_CASE,
    # concat()/concat_ws() skip NULL arguments (CONCAT_STRICT `||` propagates)
    FunctionType.CONCAT,
    FunctionType.CONCAT_WS,
}


def propagates_argument_nulls(concept: "BuildConcept") -> bool:
    if concept.derivation != Derivation.BASIC:
        return False
    lineage = concept.lineage
    if not isinstance(lineage, BuildFunction):
        return False
    return lineage.operator not in NULL_SUPPRESSING_FUNCTIONS


# Coarse semantic families for presenting the function surface grouped by
# subject (docs, agent reference). A function belongs to the first family that
# lists it; anything unlisted lands in "other".
FUNCTION_FAMILIES: list[tuple[str, frozenset[FunctionType]]] = [
    ("aggregate", frozenset(FunctionClass.AGGREGATE_FUNCTIONS.value)),
    (
        "string",
        frozenset(
            {
                FunctionType.LOWER,
                FunctionType.UPPER,
                FunctionType.LTRIM,
                FunctionType.RTRIM,
                FunctionType.TRIM,
                FunctionType.HEX,
                FunctionType.CONCAT,
                FunctionType.CONCAT_STRICT,
                FunctionType.CONCAT_WS,
                FunctionType.SPLIT,
                FunctionType.STRPOS,
                FunctionType.CONTAINS,
                FunctionType.LENGTH,
                FunctionType.SUBSTRING,
                FunctionType.REPLACE,
                FunctionType.REGEXP_CONTAINS,
                FunctionType.REGEXP_EXTRACT,
                FunctionType.REGEXP_REPLACE,
                FunctionType.FORMAT_TIME,
                FunctionType.PARSE_TIME,
            }
        ),
    ),
    (
        "date/time",
        frozenset(
            {
                FunctionType.DATE,
                FunctionType.DATETIME,
                FunctionType.TIMESTAMP,
                FunctionType.SECOND,
                FunctionType.MINUTE,
                FunctionType.HOUR,
                FunctionType.DAY,
                FunctionType.DAY_OF_WEEK,
                FunctionType.WEEK,
                FunctionType.MONTH,
                FunctionType.QUARTER,
                FunctionType.YEAR,
                FunctionType.MONTH_NAME,
                FunctionType.DAY_NAME,
                FunctionType.UNIX_TO_TIMESTAMP,
                FunctionType.DATE_PART,
                FunctionType.DATE_TRUNCATE,
                FunctionType.DATE_ADD,
                FunctionType.DATE_SUB,
                FunctionType.DATE_DIFF,
                FunctionType.DATE_SPINE,
                FunctionType.CURRENT_DATE,
                FunctionType.CURRENT_DATETIME,
                FunctionType.CURRENT_TIMESTAMP,
            }
        ),
    ),
    (
        "array/map/struct",
        frozenset(
            {
                FunctionType.UNNEST,
                FunctionType.ARRAY,
                FunctionType.ARRAY_DISTINCT,
                FunctionType.ARRAY_SUM,
                FunctionType.ARRAY_SORT,
                FunctionType.ARRAY_TO_STRING,
                FunctionType.ARRAY_TRANSFORM,
                FunctionType.ARRAY_FILTER,
                FunctionType.GENERATE_ARRAY,
                FunctionType.MAP_KEYS,
                FunctionType.MAP_VALUES,
                FunctionType.STRUCT,
            }
        ),
    ),
    (
        "math",
        frozenset(
            {
                FunctionType.ABS,
                FunctionType.SQRT,
                FunctionType.RANDOM,
                FunctionType.FLOOR,
                FunctionType.CEIL,
                FunctionType.ROUND,
                FunctionType.MOD,
                FunctionType.LOG,
                FunctionType.POWER,
                FunctionType.HASH,
            }
        ),
    ),
    (
        "conditional/cast",
        frozenset(
            {
                FunctionType.CASE,
                FunctionType.SIMPLE_CASE,
                FunctionType.COALESCE,
                FunctionType.NULLIF,
                FunctionType.IS_NULL,
                FunctionType.GREATEST,
                FunctionType.LEAST,
                FunctionType.CAST,
            }
        ),
    ),
    (
        "geo",
        frozenset(
            {
                FunctionType.GEO_FROM_TEXT,
                FunctionType.GEO_X,
                FunctionType.GEO_Y,
                FunctionType.GEO_CENTROID,
                FunctionType.GEO_POINT,
                FunctionType.GEO_DISTANCE,
                FunctionType.GEO_TRANSFORM,
            }
        ),
    ),
]


def function_family(operator: FunctionType) -> str:
    for label, members in FUNCTION_FAMILIES:
        if operator in members:
            return label
    return "other"


class FunctionFactory:
    def __init__(self, environment: Environment | None = None):
        self.environment = environment

    def create_function(
        self,
        args: list[Any],
        operator: FunctionType,
        meta: "Meta | None" = None,
    ):
        if operator not in FUNCTION_REGISTRY:
            raise ValueError(f"Function {operator} not in registry")
        config = FUNCTION_REGISTRY[operator]
        valid_inputs: VALID_INPUTS_TYPE = config.valid_inputs or set(DataType)
        output_purpose = config.output_purpose
        base_output_type = config.output_type
        arg_count = config.arg_count

        if args:
            if not self.environment:
                raise ValueError("Environment required for function creation with args")
            # TODO: remove this dependency
            from trilogy.parsing.common import process_function_args

            full_args = process_function_args(
                args, environment=self.environment, meta=meta
            )
        else:
            full_args = []
        if (
            operator == FunctionType.COUNT
            and full_args
            and self.environment is not None
            and _is_constant_count_argument(full_args[0], self.environment)
        ):
            raise InvalidSyntaxException(
                "count(<constant>) does not identify rows. Count the row grain "
                "instead, for example `count(grain(key1, key2))`; for a "
                "conditional count use `count(grain(key1, key2) ? condition)`."
            )
        final_output_type: CONCRETE_TYPES
        has_undefined = any(isinstance(x, UndefinedConcept) for x in full_args)
        try:
            if config.output_type_function:
                final_output_type = config.output_type_function(full_args)
            elif not base_output_type:
                final_output_type = merge_datatypes(
                    [arg_to_datatype(x) for x in full_args]
                )
            elif base_output_type:
                final_output_type = base_output_type
            else:
                raise SyntaxError(f"Could not determine output type for {operator}")
        except Exception:
            # An unresolved reference is deferred to an UndefinedConcept (UNKNOWN
            # type) during select parsing. Computing an output type over it can
            # raise a confusing error (e.g. coalesce's same-type check on
            # {STRING, UNKNOWN}) that masks the real problem. Defer: emit an
            # UNKNOWN-typed Function so select finalization reports the clean
            # UndefinedConceptException (with suggestions) instead. Only do this
            # when an undefined arg is actually present and was the cause —
            # functions whose output type is independent of that arg (e.g. CAST to
            # an explicit target) compute fine and keep their real type.
            if not has_undefined:
                raise
            return Function(
                operator=operator,
                arguments=full_args,  # type: ignore
                output_datatype=DataType.UNKNOWN,
                output_purpose=output_purpose or Purpose.PROPERTY,
                valid_inputs=valid_inputs,
                arg_count=arg_count,
            )
        if isinstance(final_output_type, TraitDataType) and self.environment:
            final_output_type = TraitDataType(
                type=final_output_type.type,
                traits=[
                    x
                    for x in final_output_type.traits
                    if operator
                    not in self.environment.data_types.get(
                        x, CUSTOM_PLACEHOLDER
                    ).drop_on
                ],
            )

        if operator in (FunctionType.CASE, FunctionType.SIMPLE_CASE):
            self._coerce_case_constant_branches(full_args, final_output_type)

        if not output_purpose:
            if operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                output_purpose = Purpose.METRIC
            else:
                output_purpose = Purpose.PROPERTY

        func = Function(
            operator=operator,
            arguments=full_args,  # type: ignore
            output_datatype=final_output_type,
            output_purpose=output_purpose,
            valid_inputs=valid_inputs,
            arg_count=arg_count,
        )
        func.validate_arguments()
        return func

    def _coerce_case_constant_branches(
        self, branches: list[Any], target: CONCRETE_TYPES
    ) -> None:
        """Wrap literal-constant branch results that were widened to ``target``
        in an explicit CAST, so generated SQL forces the engine type rather
        than relying on implicit coercion of the bare literal."""
        if target.data_type == DataType.UNKNOWN:
            return
        for branch in branches:
            if not isinstance(branch, (CaseWhen, CaseElse, CaseSimpleWhen)):
                continue
            if not _is_literal_constant(branch.expr):
                continue
            current = arg_to_datatype(branch.expr)
            if current == DataType.NULL or current.data_type == target.data_type:
                continue
            branch.expr = self.create_function([branch.expr, target], FunctionType.CAST)


def create_function_derived_concept(
    name: str,
    namespace: str,
    operator: FunctionType,
    arguments: list[Concept],
    environment: Environment,
    output_type: Optional[CONCRETE_TYPES] = None,
    output_purpose: Optional[Purpose] = None,
) -> Concept:
    purpose = (
        function_args_to_output_purpose(arguments, environment=environment)
        if output_purpose is None
        else output_purpose
    )
    output_type = arg_to_datatype(arguments[0]) if output_type is None else output_type
    return Concept(
        name=name,
        namespace=namespace,
        datatype=output_type,
        purpose=purpose,
        lineage=Function(
            operator=operator,
            arguments=[x.reference for x in arguments],
            output_datatype=output_type,
            output_purpose=purpose,
            arg_count=len(arguments),
        ),
    )


def argument_to_purpose(arg) -> Purpose:
    match arg:
        case UndefinedConcept():
            return Purpose.UNKNOWN

        case Function(output_purpose=purpose):
            return purpose

        case AggregateWrapper(function=f, by=by):
            # Guard logic for AggregateWrapper
            if by and f.output_purpose == Purpose.METRIC:
                return Purpose.PROPERTY
            return f.output_purpose

        case Parenthetical(content=content):
            return argument_to_purpose(content)

        case (
            NumberingWindowItem()
            | NavigationWindowItem()
            | Conditional()
            | Comparison()
            | FilterItem()
            | RowsetItem()
        ):
            return Purpose.PROPERTY

        case Concept(purpose=base, lineage=lineage):
            # Guard logic for Concept
            if (
                isinstance(lineage, AggregateWrapper)
                and lineage.by
                and base == Purpose.METRIC
            ):
                return Purpose.PROPERTY
            return base

        # Grouping all constant-like types
        case (
            int()
            | float()
            | str()
            | bool()
            | list()
            | NumericType()
            | DataType()
            | DatePart()
            | MagicConstants()
        ):
            return Purpose.CONSTANT

        case _:
            raise ValueError(f"Cannot parse arg purpose for {arg} of type {type(arg)}")


def _is_constant_count_argument(arg: Any, environment: Environment) -> bool:
    """Return whether COUNT's argument names no row identity."""
    if isinstance(arg, FilterItem):
        return _is_constant_count_argument(arg.content, environment)
    if isinstance(arg, Parenthetical):
        return _is_constant_count_argument(arg.content, environment)
    if isinstance(arg, Function):
        if arg.operator == FunctionType.UNNEST:
            return False
        return bool(arg.arguments) and all(
            _is_constant_count_argument(child, environment) for child in arg.arguments
        )
    if isinstance(arg, ConceptRef):
        concept = environment.concepts.get(arg.address)
        return concept is not None and concept.purpose == Purpose.CONSTANT
    if isinstance(arg, Concept):
        return arg.purpose == Purpose.CONSTANT
    return argument_to_purpose(arg) == Purpose.CONSTANT


def function_args_to_output_purpose(args, environment: Environment) -> Purpose:
    has_metric = False
    has_non_constant = False
    has_non_single_row_constant = False
    if not args:
        return Purpose.CONSTANT
    for arg in args:
        if isinstance(arg, ConceptRef):
            arg = environment.concepts[arg.address]
        purpose = argument_to_purpose(arg)
        if purpose == Purpose.METRIC:
            has_metric = True
        if purpose != Purpose.CONSTANT:
            has_non_constant = True
        if isinstance(arg, Concept) and arg.granularity != Granularity.SINGLE_ROW:
            has_non_single_row_constant = True
    if args and not has_non_constant and not has_non_single_row_constant:
        return Purpose.CONSTANT
    if has_metric:
        return Purpose.METRIC
    return Purpose.PROPERTY


def try_create_auto_derived(
    parent: Concept, suffix: str, environment: Environment
) -> Concept | None:
    """Try to create a derived concept from a parent and suffix.

    If the suffix matches a single-arg function valid for the parent's
    datatype, returns the derived Concept. Otherwise returns None."""
    from trilogy.core.models.author import Grain

    try:
        ftype = FunctionType(suffix)
    except ValueError:
        return None

    config = FUNCTION_REGISTRY.get(ftype)
    if not config or config.arg_count != 1:
        return None

    parent_dt = parent.output_datatype
    valid = config.valid_inputs
    if isinstance(valid, list):
        if valid and parent_dt not in valid[0]:
            return None
    elif isinstance(valid, set) and parent_dt not in valid:
        return None

    func = FunctionFactory(environment).create_function(args=[parent], operator=ftype)

    if ftype in FunctionClass.AGGREGATE_FUNCTIONS.value:
        return Concept(
            name=f"{parent.name}.{suffix}",
            datatype=func.output_datatype,
            purpose=Purpose.METRIC,
            lineage=func,
            grain=Grain(),
            namespace=parent.namespace,
            keys=set(),
        )

    purpose = (
        Purpose.CONSTANT
        if parent.purpose == Purpose.CONSTANT
        else (func.output_purpose or Purpose.PROPERTY)
    )
    return Concept(
        name=f"{parent.name}.{suffix}",
        datatype=func.output_datatype,
        purpose=purpose,
        lineage=func,
        grain=parent.grain,
        namespace=parent.namespace,
        keys=set([parent.address]),
    )
