from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Optional

from lark.tree import Meta

from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    DatePart,
    FunctionClass,
    FunctionType,
    Granularity,
    InfiniteFunctionArgs,
    Purpose,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    AggregateWrapper,
    Concept,
    Function,
    Parenthetical,
    UndefinedConcept,
    WindowItem,
)
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    DataType,
    ListType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
    arg_to_datatype,
    merge_datatypes,
)
from trilogy.core.models.environment import Environment

GENERIC_ARGS = Concept | Function | str | int | float | date | datetime


@dataclass
class FunctionConfig:
    arg_count: int = 1
    valid_inputs: set[DataType] | list[set[DataType]] | None = None
    output_purpose: Purpose | None = None
    output_type: (
        DataType | ListType | MapType | StructType | NumericType | TraitDataType | None
    ) = None
    output_type_function: Optional[Callable] = None


def get_unnest_output_type(args: list[Any]) -> CONCRETE_TYPES:
    output = arg_to_datatype(args[0])
    if isinstance(output, (ListType, MapType)):
        output = output.value_data_type
    else:
        output = DataType.STRING
    return output


def get_coalesce_output_type(args: list[Any]) -> CONCRETE_TYPES:
    non_null = [x for x in args if not x == MagicConstants.NULL]
    processed = [arg_to_datatype(x) for x in non_null if x]
    if not len(set(processed)) == 1:
        raise InvalidSyntaxException(
            f"All arguments to coalesce must be of the same type, have {set(arg_to_datatype(x) for x in args)}"
        )
    return processed[0]


def get_index_output_type(
    args: list[Any],
) -> CONCRETE_TYPES:
    arg = args[0]
    datatype = arg_to_datatype(arg)
    if isinstance(datatype, ListType):
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
        return arg_to_datatype(datatype.fields_map[lookup])
    return datatype


def get_cast_output_type(
    args: list[Any],
) -> DataType:
    return args[1]


def get_output_type_at_index(args, index: int):
    return arg_to_datatype(args[index])


def validate_case_output(
    args: list[Any],
) -> DataType:
    datatypes = set()
    mapz = dict()
    for arg in args:
        output_datatype = arg_to_datatype(arg.expr)
        if output_datatype != DataType.NULL:
            datatypes.add(output_datatype.data_type)
        mapz[str(arg.expr)] = output_datatype
    if not len(datatypes) == 1:
        raise SyntaxError(
            f"All case expressions must have the same output datatype, got {datatypes} from {mapz}"
        )
    return datatypes.pop()


def create_struct_output(
    args: list[Any],
) -> StructType:
    zipped = dict(zip(args[::2], args[1::2]))
    types = [arg_to_datatype(x) for x in args[1::2]]
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
    else:
        raise InvalidSyntaxException(f"Date truncation not supported for {target}")


FUNCTION_REGISTRY: dict[FunctionType, FunctionConfig] = {
    FunctionType.ALIAS: FunctionConfig(
        arg_count=1,
    ),
    FunctionType.PARENTHETICAL: FunctionConfig(
        arg_count=1,
    ),
    FunctionType.UNNEST: FunctionConfig(
        valid_inputs={
            DataType.ARRAY,
            DataType.LIST,
        },
        output_purpose=Purpose.KEY,
        output_type_function=get_unnest_output_type,
        arg_count=1,
    ),
    FunctionType.GROUP: FunctionConfig(
        arg_count=-1,
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
    FunctionType.MAX: FunctionConfig(
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.DATE,
            DataType.DATETIME,
            DataType.TIMESTAMP,
            DataType.BOOL,
        },
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.MIN: FunctionConfig(
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.DATE,
            DataType.DATETIME,
            DataType.TIMESTAMP,
        },
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.SPLIT: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=ListType(type=DataType.STRING),
        arg_count=2,
    ),
    FunctionType.INDEX_ACCESS: FunctionConfig(
        valid_inputs=[
            {
                DataType.LIST,
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
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.PROPERTY,
        arg_count=1,
    ),
    FunctionType.COALESCE: FunctionConfig(
        valid_inputs={*DataType},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=-1,
        output_type_function=get_coalesce_output_type,
    ),
    FunctionType.CURRENT_DATE: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        output_type=DataType.DATE,
        arg_count=0,
    ),
    FunctionType.CURRENT_DATETIME: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        output_type=DataType.DATE,
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
    FunctionType.CONTAINS: FunctionConfig(
        valid_inputs=[
            {DataType.STRING},
            {DataType.STRING},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=2,
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
    FunctionType.LIKE: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=2,
    ),
    FunctionType.ILIKE: FunctionConfig(
        valid_inputs={DataType.STRING},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=2,
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
                DataType.STRING,
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
                DataType.STRING,
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
            DataType.STRING,
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
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.NUMERIC,
        },
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.SUBTRACT: FunctionConfig(
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.NUMERIC,
        },
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.MULTIPLY: FunctionConfig(
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.NUMERIC,
        },
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.DIVIDE: FunctionConfig(
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.NUMERIC,
        },
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.MOD: FunctionConfig(
        valid_inputs=[
            {DataType.INTEGER, DataType.FLOAT, DataType.NUMBER, DataType.NUMERIC},
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.ROUND: FunctionConfig(
        valid_inputs=[
            {DataType.INTEGER, DataType.FLOAT, DataType.NUMBER, DataType.NUMERIC},
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.PROPERTY,
        output_type_function=lambda args: get_output_type_at_index(args, 0),
        arg_count=2,
    ),
    FunctionType.CUSTOM: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
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
    FunctionType.CONSTANT: FunctionConfig(
        output_purpose=Purpose.CONSTANT,
        arg_count=1,
    ),
    FunctionType.IS_NULL: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.BOOL,
        arg_count=1,
    ),
    FunctionType.STRUCT: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
        output_type_function=create_struct_output,
    ),
    FunctionType.ARRAY: FunctionConfig(
        output_purpose=Purpose.PROPERTY,
        arg_count=InfiniteFunctionArgs,
        output_type=ListType(type=DataType.STRING),
    ),
    FunctionType.LENGTH: FunctionConfig(
        valid_inputs={DataType.STRING, DataType.ARRAY, DataType.MAP},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.SUM: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.AVG: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
        arg_count=1,
    ),
    FunctionType.UNIX_TO_TIMESTAMP: FunctionConfig(
        valid_inputs={DataType.INTEGER},
        output_purpose=Purpose.PROPERTY,
        output_type=DataType.TIMESTAMP,
        arg_count=1,
    ),
}

EXCLUDED_FUNCTIONS = {
    FunctionType.CUSTOM,
    # Temporary
    FunctionType.DATE_LITERAL,
    FunctionType.DATETIME_LITERAL,
    FunctionType.ARRAY,
}

for k in FunctionType.__members__.values():
    if k not in FUNCTION_REGISTRY and k not in EXCLUDED_FUNCTIONS:
        raise InvalidSyntaxException(
            f"Function enum value {k} not in creation registry"
        )


class FunctionFactory:
    def __init__(self, environment: Environment | None = None):
        self.environment = environment

    def create_function(
        self,
        args: list[Any],
        operator: FunctionType,
        meta: Meta | None = None,
    ):
        if operator not in FUNCTION_REGISTRY:
            raise ValueError(f"Function {operator} not in registry")
        config = FUNCTION_REGISTRY[operator]
        valid_inputs: set[DataType] | list[set[DataType]] = config.valid_inputs or set(
            DataType
        )
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
        final_output_type: CONCRETE_TYPES
        if config.output_type_function:

            final_output_type = config.output_type_function(full_args)
        elif not base_output_type:

            final_output_type = merge_datatypes([arg_to_datatype(x) for x in full_args])
        elif base_output_type:
            final_output_type = base_output_type
        else:
            raise SyntaxError(f"Could not determine output type for {operator}")
        if not output_purpose:
            if operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                output_purpose = Purpose.METRIC
            else:
                output_purpose = Purpose.PROPERTY
        return Function(
            operator=operator,
            arguments=full_args,
            output_datatype=final_output_type,
            output_purpose=output_purpose,
            valid_inputs=valid_inputs,
            arg_count=arg_count,
        )


def create_function_derived_concept(
    name: str,
    namespace: str,
    operator: FunctionType,
    arguments: list[Concept],
    output_type: Optional[
        DataType | ListType | StructType | MapType | NumericType | TraitDataType
    ] = None,
    output_purpose: Optional[Purpose] = None,
) -> Concept:
    purpose = (
        function_args_to_output_purpose(arguments)
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
            arguments=arguments,
            output_datatype=output_type,
            output_purpose=purpose,
            arg_count=len(arguments),
        ),
    )


def argument_to_purpose(arg) -> Purpose:
    if isinstance(arg, UndefinedConcept):
        return Purpose.UNKNOWN
    if isinstance(arg, Function):
        return arg.output_purpose
    elif isinstance(arg, AggregateWrapper):
        base = arg.function.output_purpose
        if arg.by and base == Purpose.METRIC:
            return Purpose.PROPERTY
        return arg.function.output_purpose
    elif isinstance(arg, Parenthetical):
        return argument_to_purpose(arg.content)
    elif isinstance(arg, WindowItem):
        return Purpose.PROPERTY
    elif isinstance(arg, Concept):
        base = arg.purpose
        if (
            isinstance(arg.lineage, AggregateWrapper)
            and arg.lineage.by
            and base == Purpose.METRIC
        ):
            return Purpose.PROPERTY
        return arg.purpose
    elif isinstance(arg, (int, float, str, bool, list, NumericType, DataType)):
        return Purpose.CONSTANT
    elif isinstance(arg, DatePart):
        return Purpose.CONSTANT
    elif isinstance(arg, MagicConstants):
        return Purpose.CONSTANT
    else:
        raise ValueError(f"Cannot parse arg purpose for {arg} of type {type(arg)}")


def function_args_to_output_purpose(args) -> Purpose:
    has_metric = False
    has_non_constant = False
    has_non_single_row_constant = False
    if not args:
        return Purpose.CONSTANT
    for arg in args:
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


def Count(args: list[Concept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def CountDistinct(args: list[Concept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def Max(args: list[Concept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def Min(args: list[Concept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def Split(args: list[Concept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.SPLIT
    )


def AttrAccess(args: list[GENERIC_ARGS], environment: Environment):
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.ATTR_ACCESS
    )


def CurrentDate(
    args: list[Concept], environment: Environment | None = None
) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.CURRENT_DATE
    )


def CurrentDatetime(
    args: list[Concept], environment: Environment | None = None
) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.CURRENT_DATETIME
    )
