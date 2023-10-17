from preql.core.models import Function
from preql.core.enums import FunctionType, DataType, Purpose


def Count(args) -> Function:
    return Function(
        operator=FunctionType.COUNT,
        arguments=args,
        output_datatype=DataType.INTEGER,
        output_purpose=Purpose.METRIC,
        arg_count=1,
    )


def CountDistinct(args) -> Function:
    return Function(
        operator=FunctionType.COUNT_DISTINCT,
        arguments=args,
        output_datatype=DataType.INTEGER,
        output_purpose=Purpose.METRIC,
        arg_count=1,
    )


def Max(args) -> Function:
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


def Min(args) -> Function:
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


def Split(args) -> Function:
    # TODO: overload this for non-string types?
    return Function(
        operator=FunctionType.SPLIT,
        arguments=args,
        # first arg sets properties
        output_datatype=DataType.ARRAY,
        output_purpose=args[0].purpose,
        valid_inputs={DataType.STRING},
        arg_count=2,
    )


def IndexAccess(args):
    return Function(
        operator=FunctionType.INDEX_ACCESS,
        arguments=args,
        # first arg sets properties
        # TODO: THIS IS WRONG - figure out how to get at array types
        output_datatype=DataType.STRING,
        # force this to a key
        output_purpose=Purpose.KEY,
        valid_inputs=[DataType.ARRAY, DataType.INTEGER],
        arg_count=2,
    )
