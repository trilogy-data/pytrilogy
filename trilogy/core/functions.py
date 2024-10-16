from trilogy.core.models import (
    Function,
    Concept,
    AggregateWrapper,
    Parenthetical,
    arg_to_datatype,
    WindowItem,
    DataType,
    ListType,
    StructType,
    MapType,
    NumericType,
)
from trilogy.core.enums import FunctionType, Purpose, Granularity, DatePart
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.constants import MagicConstants
from typing import Optional


def create_function_derived_concept(
    name: str,
    namespace: str,
    operator: FunctionType,
    arguments: list[Concept],
    output_type: Optional[
        DataType | ListType | StructType | MapType | NumericType
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


def Unnest(args: list[Concept]) -> Function:
    output = arg_to_datatype(args[0])
    if isinstance(output, (ListType)):
        output = output.value_data_type
    else:
        output = DataType.STRING
    return Function(
        operator=FunctionType.UNNEST,
        arguments=args,
        output_datatype=output,
        output_purpose=Purpose.KEY,
        arg_count=1,
        valid_inputs={
            DataType.ARRAY,
            DataType.LIST,
            ListType(type=DataType.STRING),
            ListType(type=DataType.INTEGER),
        },
    )


def Group(args: list[Concept]) -> Function:
    output = args[0]
    return Function(
        operator=FunctionType.GROUP,
        arguments=args,
        output_datatype=output.datatype,
        output_purpose=Purpose.PROPERTY,
        arg_count=-1,
    )


def Count(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.COUNT,
        arguments=args,
        output_datatype=DataType.INTEGER,
        output_purpose=Purpose.METRIC,
        arg_count=1,
    )


def CountDistinct(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.COUNT_DISTINCT,
        arguments=args,
        output_datatype=DataType.INTEGER,
        output_purpose=Purpose.METRIC,
        arg_count=1,
    )


def Max(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.MAX,
        arguments=args,
        output_datatype=args[0].datatype,
        output_purpose=Purpose.METRIC,
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.DATE,
            DataType.DATETIME,
            DataType.TIMESTAMP,
            DataType.BOOL,
        },
        arg_count=1,
        # output_grain=Grain(components=arguments),
    )


def Min(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.MIN,
        arguments=args,
        output_datatype=args[0].datatype,
        output_purpose=Purpose.METRIC,
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
            DataType.DATE,
            DataType.DATETIME,
            DataType.TIMESTAMP,
        },
        arg_count=1,
        # output_grain=Grain(components=arguments),
    )


def Split(args: list[Concept]) -> Function:
    # TODO: overload this for non-string types?
    return Function(
        operator=FunctionType.SPLIT,
        arguments=args,
        # first arg sets properties
        output_datatype=ListType(type=DataType.STRING),
        output_purpose=function_args_to_output_purpose(args),
        valid_inputs={DataType.STRING},
        arg_count=2,
    )


def get_index_output_type(
    arg: Concept,
) -> DataType | StructType | MapType | ListType | NumericType:
    if isinstance(arg.datatype, ListType):
        return arg.datatype.value_data_type
    elif isinstance(arg.datatype, MapType):
        return arg.datatype.value_data_type
    return arg.datatype


def IndexAccess(args: list[Concept]):
    return Function(
        operator=FunctionType.INDEX_ACCESS,
        arguments=args,
        output_datatype=get_index_output_type(args[0]),
        output_purpose=Purpose.PROPERTY,
        valid_inputs=[
            {
                DataType.LIST,
                ListType(type=DataType.STRING),
                ListType(type=DataType.INTEGER),
            },
            {
                DataType.INTEGER,
            },
        ],
        arg_count=2,
    )


def MapAccess(args: list[Concept]):
    return Function(
        operator=FunctionType.MAP_ACCESS,
        arguments=args,
        output_datatype=get_index_output_type(args[0]),
        output_purpose=Purpose.PROPERTY,
        valid_inputs=[
            {
                DataType.MAP,
            },
            {
                DataType.INTEGER,
                DataType.STRING,
            },
        ],
        arg_count=2,
    )


def get_attr_datatype(
    arg: Concept, lookup
) -> DataType | ListType | StructType | MapType | NumericType:
    if isinstance(arg.datatype, StructType):
        return arg_to_datatype(arg.datatype.fields_map[lookup])
    return arg.datatype


def AttrAccess(args: list[Concept]):
    return Function(
        operator=FunctionType.ATTR_ACCESS,
        arguments=args,
        output_datatype=get_attr_datatype(args[0], args[1]),  # type: ignore
        output_purpose=Purpose.PROPERTY,
        valid_inputs=[
            {DataType.STRUCT},
            {
                DataType.STRING,
            },
        ],
        arg_count=2,
    )


def Abs(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.ABS,
        arguments=args,
        output_datatype=args[0].datatype,
        output_purpose=function_args_to_output_purpose(args),
        valid_inputs={
            DataType.INTEGER,
            DataType.FLOAT,
            DataType.NUMBER,
        },
        arg_count=1,
        # output_grain=Grain(components=arguments),
    )


def Coalesce(args: list[Concept]) -> Function:
    non_null = [x for x in args if not x == MagicConstants.NULL]
    if not len(set(arg_to_datatype(x) for x in non_null if x)) == 1:
        raise InvalidSyntaxException(
            f"All arguments to coalesce must be of the same type, have {set(arg_to_datatype(x) for x in args)}"
        )
    return Function(
        operator=FunctionType.COALESCE,
        arguments=args,
        output_datatype=arg_to_datatype(non_null[0]),
        output_purpose=function_args_to_output_purpose(non_null),
        arg_count=-1,
        # output_grain=Grain(components=arguments),
    )


def CurrentDate(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.CURRENT_DATE,
        arguments=args,
        output_datatype=DataType.DATE,
        output_purpose=Purpose.CONSTANT,
        arg_count=0,
        # output_grain=Grain(components=arguments),
    )


def CurrentDatetime(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.CURRENT_DATETIME,
        arguments=args,
        output_datatype=DataType.DATE,
        output_purpose=Purpose.CONSTANT,
        arg_count=0,
        # output_grain=Grain(components=arguments),
    )


def IsNull(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.IS_NULL,
        arguments=args,
        output_datatype=DataType.BOOL,
        output_purpose=function_args_to_output_purpose(args),
        arg_count=1,
        # output_grain=Grain(components=arguments),
    )


def Bool(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.BOOL,
        arguments=args,
        output_datatype=DataType.BOOL,
        output_purpose=function_args_to_output_purpose(args),
        arg_count=1,
        # output_grain=Grain(components=arguments),
    )


def StrPos(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.STRPOS,
        arguments=args,
        output_datatype=DataType.INTEGER,
        output_purpose=function_args_to_output_purpose(args),
        arg_count=2,
        valid_inputs=[
            {DataType.STRING},
            {DataType.STRING},
        ],
    )


def SubString(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.SUBSTRING,
        arguments=args,
        output_datatype=DataType.STRING,
        output_purpose=function_args_to_output_purpose(args),
        arg_count=3,
        valid_inputs=[{DataType.STRING}, {DataType.INTEGER}, {DataType.INTEGER}],
    )
