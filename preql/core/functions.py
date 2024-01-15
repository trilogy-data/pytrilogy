from preql.core.models import (
    Function,
    Concept,
    AggregateWrapper,
    Parenthetical,
    arg_to_datatype,
)
from preql.core.enums import FunctionType, DataType, Purpose
from preql.core.exceptions import InvalidSyntaxException
from preql.constants import MagicConstants


def argument_to_purpose(arg) -> Purpose:
    if isinstance(arg, Function):
        return arg.output_purpose
    elif isinstance(arg, AggregateWrapper):
        return arg.function.output_purpose
    elif isinstance(arg, Parenthetical):
        return argument_to_purpose(arg.content)
    elif isinstance(arg, Concept):
        return arg.purpose
    elif isinstance(arg, (int, float, str, bool, list)):
        return Purpose.CONSTANT
    elif isinstance(arg, DataType):
        return Purpose.CONSTANT
    elif isinstance(arg, MagicConstants):
        return Purpose.CONSTANT
    else:
        raise ValueError(f"Cannot parse arg type for {arg} type {type(arg)}")


def function_args_to_output_purpose(args) -> Purpose:
    has_metric = False
    has_non_constant = False
    for arg in args:
        purpose = argument_to_purpose(arg)
        if purpose == Purpose.METRIC:
            has_metric = True
        if purpose != Purpose.CONSTANT:
            has_non_constant = True
    if not has_non_constant:
        return Purpose.CONSTANT
    if has_metric:
        return Purpose.METRIC
    return Purpose.PROPERTY


def Unnest(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.UNNEST,
        arguments=args,
        output_datatype=args[0].datatype,
        output_purpose=Purpose.KEY,
        arg_count=1,
        valid_inputs={DataType.ARRAY},
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
        },
        arg_count=1
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
        arg_count=1
        # output_grain=Grain(components=arguments),
    )


def Split(args: list[Concept]) -> Function:
    # TODO: overload this for non-string types?
    return Function(
        operator=FunctionType.SPLIT,
        arguments=args,
        # first arg sets properties
        output_datatype=DataType.ARRAY,
        output_purpose=function_args_to_output_purpose(args),
        valid_inputs={DataType.STRING},
        arg_count=2,
    )


def IndexAccess(args: list[Concept]):
    return Function(
        operator=FunctionType.INDEX_ACCESS,
        arguments=args,
        # first arg sets properties
        # TODO: THIS IS WRONG - figure out how to get at array types
        output_datatype=DataType.STRING,
        # force this to a key
        output_purpose=Purpose.PROPERTY,
        valid_inputs={DataType.ARRAY, DataType.INTEGER},
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
        arg_count=1
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
        arg_count=-1
        # output_grain=Grain(components=arguments),
    )


def CurrentDate(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.CURRENT_DATE,
        arguments=args,
        output_datatype=DataType.DATE,
        output_purpose=Purpose.CONSTANT,
        arg_count=0
        # output_grain=Grain(components=arguments),
    )


def CurrentDatetime(args: list[Concept]) -> Function:
    return Function(
        operator=FunctionType.CURRENT_DATETIME,
        arguments=args,
        output_datatype=DataType.DATE,
        output_purpose=Purpose.CONSTANT,
        arg_count=0
        # output_grain=Grain(components=arguments),
    )
