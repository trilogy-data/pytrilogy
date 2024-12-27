from datetime import date, datetime
from typing import Optional

from trilogy.constants import MagicConstants
from trilogy.core.enums import DatePart, FunctionType, Granularity, Purpose
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.execute_models import (
    BoundConcept,
    DataType,
    ListType,
    MapType,
    NumericType,
    Parenthetical,
    StructType,
    arg_to_datatype,
    AggregateWrapper,
    WindowItem,
    Function,
    BoundEnvironment,
    Reference
)

from trilogy.core.author_models import (
    AggregateWrapperRef,
    FunctionRef,
    WindowItemRef,
    ConceptRef,

)

GENERIC_ARGS = ConceptRef | FunctionRef | str | int | float | date | datetime




def create_function_derived_concept(
    name: str,
    namespace: str,
    operator: FunctionType,
    arguments: list[BoundConcept],
    output_type: Optional[
        DataType | ListType | StructType | MapType | NumericType
    ] = None,
    output_purpose: Optional[Purpose] = None,
) -> BoundConcept:
    purpose = (
        function_args_to_output_purpose(arguments)
        if output_purpose is None
        else output_purpose
    )
    output_type = arg_to_datatype(arguments[0]) if output_type is None else output_type
    return BoundConcept(
        name=name,
        namespace=namespace,
        datatype=output_type,
        purpose=purpose,
        lineage=FunctionRef(
            operator=operator,
            arguments=arguments,
            output_datatype=output_type,
            output_purpose=purpose,
            arg_count=len(arguments),
        ),
    )


def argument_to_purpose(arg, environment:BoundEnvironment) -> Purpose:
    if isinstance(arg, Reference):
        return argument_to_purpose(arg.instantiate(environment), environment)
    if isinstance(arg, Function):
        return arg.output_purpose
    elif isinstance(arg, AggregateWrapper):
        base = arg.function.output_purpose
        if arg.by and base == Purpose.METRIC:
            return Purpose.PROPERTY
        return arg.function.output_purpose
    elif isinstance(arg, Parenthetical):
        return argument_to_purpose(arg.content, environment)
    elif isinstance(arg, WindowItem):
        return Purpose.PROPERTY
    elif isinstance(arg, BoundConcept):
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


def function_args_to_output_purpose(args,  environment:BoundEnvironment) -> Purpose:
    has_metric = False
    has_non_constant = False
    has_non_single_row_constant = False
    if not args:
        return Purpose.CONSTANT
    for arg in args:
        purpose = argument_to_purpose(arg, environment)
        if purpose == Purpose.METRIC:
            has_metric = True
        if purpose != Purpose.CONSTANT:
            has_non_constant = True
        if isinstance(arg, BoundConcept) and arg.granularity != Granularity.SINGLE_ROW:
            has_non_single_row_constant = True
    if args and not has_non_constant and not has_non_single_row_constant:
        return Purpose.CONSTANT
    if has_metric:
        return Purpose.METRIC
    return Purpose.PROPERTY


def Unnest(args: list[BoundConcept], environment:BoundEnvironment) -> Function:
    output = arg_to_datatype(args[0], environment)
    if isinstance(output, (ListType)):
        output = output.value_data_type
    else:
        output = DataType.STRING
    return FunctionRef(
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


def Group(args: list[BoundConcept], environment:BoundEnvironment) -> Function:
    output = args[0]
    datatype = arg_to_datatype(output, environment=environment)
    return FunctionRef(
        operator=FunctionType.GROUP,
        arguments=args,
        output_datatype=datatype,
        output_purpose=Purpose.PROPERTY,
        arg_count=-1,
    )


def Count(args: list[BoundConcept]) -> Function:
    return FunctionRef(
        operator=FunctionType.COUNT,
        arguments=args,
        output_datatype=DataType.INTEGER,
        output_purpose=Purpose.METRIC,
        arg_count=1,
    )


def CountDistinct(args: list[BoundConcept]) -> Function:
    return FunctionRef(
        operator=FunctionType.COUNT_DISTINCT,
        arguments=args,
        output_datatype=DataType.INTEGER,
        output_purpose=Purpose.METRIC,
        arg_count=1,
    )


def Max(args: list[BoundConcept]) -> Function:
    return FunctionRef(
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


def Min(args: list[BoundConcept]) -> Function:
    return FunctionRef(
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


def Split(args: list[ConceptRef], environment) -> Function:
    # TODO: overload this for non-string types?
    return FunctionRef(
        operator=FunctionType.SPLIT,
        arguments=args,
        # first arg sets properties
        output_datatype=ListType(type=DataType.STRING),
        output_purpose=function_args_to_output_purpose(args, environment),
        valid_inputs={DataType.STRING},
        arg_count=2,
    )


def get_index_output_type(
    arg: BoundConcept,
    environment:BoundEnvironment
) -> DataType | StructType | MapType | ListType | NumericType:
    if isinstance(arg, Reference):
        arg = arg.instantiate(environment)
    if isinstance(arg.datatype, ListType):
        return arg.datatype.value_data_type
    elif isinstance(arg.datatype, MapType):
        return arg.datatype.value_data_type
    return arg.datatype


def IndexAccess(args: list[BoundConcept], environment:BoundEnvironment) -> FunctionRef:
    return FunctionRef(
        operator=FunctionType.INDEX_ACCESS,
        arguments=args,
        output_datatype=get_index_output_type(args[0], environment),
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


def MapAccess(args: list[BoundConcept]):
    return FunctionRef(
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
    arg: BoundConcept, lookup
) -> DataType | ListType | StructType | MapType | NumericType:
    if isinstance(arg.datatype, StructType):
        return arg_to_datatype(arg.datatype.fields_map[lookup])
    return arg.datatype


def AttrAccess(args: list[GENERIC_ARGS]):
    return FunctionRef(
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


def Abs(args: list[BoundConcept]) -> Function:
    return FunctionRef(
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


def Coalesce(args: list[BoundConcept]) -> Function:
    non_null = [x for x in args if not x == MagicConstants.NULL]
    if not len(set(arg_to_datatype(x) for x in non_null if x)) == 1:
        raise InvalidSyntaxException(
            f"All arguments to coalesce must be of the same type, have {set(arg_to_datatype(x) for x in args)}"
        )
    return FunctionRef(
        operator=FunctionType.COALESCE,
        arguments=args,
        output_datatype=arg_to_datatype(non_null[0]),
        output_purpose=function_args_to_output_purpose(non_null),
        arg_count=-1,
        # output_grain=Grain(components=arguments),
    )


def CurrentDate(args: list[BoundConcept]) -> Function:
    return FunctionRef(
        operator=FunctionType.CURRENT_DATE,
        arguments=args,
        output_datatype=DataType.DATE,
        output_purpose=Purpose.CONSTANT,
        arg_count=0,
        # output_grain=Grain(components=arguments),
    )


def CurrentDatetime(args: list[BoundConcept]) -> Function:
    return Function(
        operator=FunctionType.CURRENT_DATETIME,
        arguments=args,
        output_datatype=DataType.DATE,
        output_purpose=Purpose.CONSTANT,
        arg_count=0,
        # output_grain=Grain(components=arguments),
    )


def IsNull(args: list[BoundConcept]) -> Function:
    return FunctionRef(
        operator=FunctionType.IS_NULL,
        arguments=args,
        output_datatype=DataType.BOOL,
        output_purpose=function_args_to_output_purpose(args),
        arg_count=1,
        # output_grain=Grain(components=arguments),
    )


def Bool(args: list[BoundConcept], environment:BoundEnvironment) -> Function:
    return FunctionRef(
        operator=FunctionType.BOOL,
        arguments=args,
        output_datatype=DataType.BOOL,
        output_purpose=function_args_to_output_purpose(args, environment),
        arg_count=1,
        # output_grain=Grain(components=arguments),
    )


def StrPos(args: list[BoundConcept]) -> Function:
    return FunctionRef(
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


def SubString(args: list[BoundConcept], environment:BoundEnvironment) -> Function:
    return FunctionRef(
        operator=FunctionType.SUBSTRING,
        arguments=args,
        output_datatype=DataType.STRING,
        output_purpose=function_args_to_output_purpose(args, environment),
        arg_count=3,
        valid_inputs=[{DataType.STRING}, {DataType.INTEGER}, {DataType.INTEGER}],
    )
