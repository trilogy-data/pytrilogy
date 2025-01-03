from datetime import date, datetime
from typing import Optional

from trilogy.constants import MagicConstants
from trilogy.core.enums import DatePart, FunctionType, Granularity, Purpose
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.core_models import arg_to_datatype
from trilogy.core.execute_models import (
    BoundConcept,
    DataType,
    ListType,
    MapType,
    NumericType,
    BoundParenthetical,
    StructType,
    BoundAggregateWrapper,
    BoundWindowItem,
    BoundFunction,
    BoundEnvironment,
    Reference,
    Meta,
)

from trilogy.core.author_models import (
    AggregateWrapper,
    Function,
    WindowItem,
    ConceptRef,
    Concept,
    Environment,
)

from trilogy.core.enums import FunctionClass, InfiniteFunctionArgs
from typing import Any, List, Union
from trilogy.parsing.common import (
    agg_wrapper_to_concept,
    arbitrary_to_concept,
    constant_to_concept,
    filter_item_to_concept,
    function_to_concept,
    process_function_args,
    window_item_to_concept,
)

from trilogy.core.core_models import (
    dict_to_map_wrapper,
    list_to_wrapper,
    tuple_to_wrapper,
    ListWrapper,
    MapWrapper,
    ListType,
    TupleWrapper,
    StructType,
    MapType,
    merge_datatypes,
    arg_to_datatype,
)

GENERIC_ARGS = ConceptRef | Function | str | int | float | date | datetime

from dataclasses import dataclass


@dataclass
class FunctionConfig:

    arg_count: int = 1
    valid_inputs: set[DataType] | list[set[DataType]] | None = None
    output_purpose: Purpose | None = None
    output_type: DataType | ListType | MapType | StructType | None = None
    output_type_function: Optional[callable] = None


def get_unnest_output_type(args: list[Any]) -> DataType:
    output = arg_to_datatype(args[0])
    if isinstance(output, (ListType,)):
        output = output.value_data_type
    else:
        output = DataType.STRING
    return output


def get_coalesce_output_type(args: list[Any]) -> DataType:
    non_null = [x for x in args if not x == MagicConstants.NULL]
    processed = [arg_to_datatype(x) for x in non_null if x]
    if not len(set(processed)) == 1:
        raise InvalidSyntaxException(
            f"All arguments to coalesce must be of the same type, have {set(arg_to_datatype(x) for x in args)}"
        )
    return processed[0]


def get_index_output_type(
    args: list[Any],
) -> DataType | StructType | MapType | ListType | NumericType:
    arg = args[0]
    datatype = arg_to_datatype(arg)
    if isinstance(datatype, ListType):
        return datatype.value_data_type
    elif isinstance(datatype, MapType):
        return datatype.value_data_type
    return datatype


def get_attr_datatype(
    args: list[Any],
) -> DataType | ListType | StructType | MapType | NumericType:
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


def validate_case_output(
    args: list[Any],
) -> DataType:
    datatypes = set()
    mapz = dict()
    for arg in args:
        output_datatype = arg_to_datatype(arg.expr)
        if output_datatype != DataType.NULL:
            datatypes.add(output_datatype)
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.COALESCE: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.ADD: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.SUBTRACT: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.MULTIPLY: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.DIVIDE: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.MOD: FunctionConfig(
        valid_inputs=[
            {DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
        arg_count=InfiniteFunctionArgs,
    ),
    FunctionType.ROUND: FunctionConfig(
        valid_inputs=[
            {DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
            {DataType.INTEGER},
        ],
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
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
        output_type=DataType.INTEGER,
        arg_count=1,
    ),
    FunctionType.AVG: FunctionConfig(
        valid_inputs={DataType.INTEGER, DataType.FLOAT, DataType.NUMBER},
        output_purpose=Purpose.METRIC,
        output_type=DataType.INTEGER,
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
    FunctionType.ALIAS,
    FunctionType.PARENTHETICAL,
    # Temporary
    FunctionType.DATE_LITERAL,
    FunctionType.DATETIME_LITERAL,
    FunctionType.ARRAY,
}

for k in FunctionType.__members__.values():
    if k not in FUNCTION_REGISTRY and k not in EXCLUDED_FUNCTIONS:
        raise InvalidSyntaxException(f"Function {k} not in registry")


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
        valid_inputs = config.valid_inputs
        output_purpose = config.output_purpose
        output_type = config.output_type
        arg_count = config.arg_count

        if args:
            if not self.environment:
                raise ValueError("Environment required for function creation with args")
            full_args = process_function_args(
                args, environment=self.environment, meta=meta
            )
        else:
            full_args = []
        if config.output_type_function:
            output_type = config.output_type_function(full_args)
        if not output_type:
            output_type = merge_datatypes([arg_to_datatype(x) for x in full_args])
        if not output_purpose:
            if operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                output_purpose = Purpose.METRIC
            else:
                output_purpose = Purpose.PROPERTY
        final_args = []
        for arg in full_args:
            if isinstance(arg, Concept):
                final_args.append(arg.reference)
            else:
                final_args.append(arg)
        return Function(
            operator=operator,
            arguments=final_args,
            output_datatype=output_type,
            output_purpose=output_purpose,
            valid_inputs=valid_inputs,
            arg_count=arg_count,
        )


def create_function_derived_concept(
    name: str,
    namespace: str,
    operator: FunctionType,
    arguments: list[BoundConcept],
    environment: Environment,
) -> Concept:
    from trilogy.parsing.common import function_to_concept

    function = FunctionFactory(environment).create_function(
        args=arguments,
        operator=operator,
    )
    return function_to_concept(
        parent=function,
        name=name,
        namespace=namespace,
        environment=environment,
    )


def Unnest(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args,
        operator=FunctionType.UNNEST,
    )


def Group(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args,
        operator=FunctionType.GROUP,
    )


def Count(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def CountDistinct(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def Max(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def Min(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COUNT
    )


def Split(args: list[ConceptRef], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.SPLIT
    )


def IndexAccess(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.INDEX_ACCESS
    )


def MapAccess(args: list[BoundConcept], environment: Environment):
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.MAP_ACCESS
    )


def AttrAccess(args: list[GENERIC_ARGS], environment: Environment):
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.ATTR_ACCESS
    )


def Abs(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.ABS
    )


def Coalesce(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.COALESCE
    )


def CurrentDate(
    args: list[BoundConcept], environment: Environment | None = None
) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.CURRENT_DATE
    )


def CurrentDatetime(
    args: list[BoundConcept], environment: Environment | None = None
) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.CURRENT_DATETIME
    )


def Bool(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.BOOL
    )


def StrPos(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.STRPOS
    )


def SubString(args: list[BoundConcept], environment: Environment) -> Function:
    return FunctionFactory(environment).create_function(
        args=args, operator=FunctionType.STRPOS
    )
