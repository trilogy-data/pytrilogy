from __future__ import annotations

from abc import ABC
from collections import UserDict, UserList
from datetime import date, datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    get_args,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    field_validator,
)
from pydantic_core import core_schema

from trilogy.constants import (
    MagicConstants,
)


class DataTyped(ABC):

    # this is not abstract
    # only because when it's a pydantic property, it fails validation
    @property
    def output_datatype(self) -> CONCRETE_TYPES:  # type: ignore
        """
        This is a huge hack to get property vs pydantic attribute inheritance to work.
        """
        if "output_datatype" in self.__dict__:
            return self.__dict__["output_datatype"]
        raise NotImplementedError


class Addressable(ABC):

    @property
    def _address(self):
        return self.address


TYPEDEF_TYPES = Union[
    "DataType",
    "MapType",
    "ListType",
    "NumericType",
    "StructType",
    "DataTyped",
    "TraitDataType",
]

CONCRETE_TYPES = Union[
    "DataType",
    "MapType",
    "ListType",
    "NumericType",
    "StructType",
    "TraitDataType",
]

KT = TypeVar("KT")
VT = TypeVar("VT")
LT = TypeVar("LT")


class DataType(Enum):
    # PRIMITIVES
    STRING = "string"
    BOOL = "bool"
    MAP = "map"
    LIST = "list"
    NUMBER = "number"
    FLOAT = "float"
    NUMERIC = "numeric"
    INTEGER = "int"
    BIGINT = "bigint"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    DATE_PART = "date_part"
    STRUCT = "struct"
    NULL = "null"

    # GRANULAR
    UNIX_SECONDS = "unix_seconds"

    # PARSING
    UNKNOWN = "unknown"

    @property
    def data_type(self):
        return self


class TraitDataType(BaseModel):
    type: DataType | NumericType | StructType | ListType | MapType
    traits: list[str]

    def __hash__(self):
        return hash(self.type)

    def __str__(self) -> str:
        return f"Trait<{self.type}, {self.traits}>"

    def __eq__(self, other):
        if isinstance(other, DataType):
            return self.type == other
        elif isinstance(other, TraitDataType):
            return self.type == other.type and self.traits == other.traits
        return False

    @property
    def data_type(self):
        return self.type

    @property
    def value(self):
        return self.data_type.value


class NumericType(BaseModel):
    precision: int = 20
    scale: int = 5

    def __str__(self) -> str:
        return f"Numeric({self.precision},{self.scale})"

    @property
    def data_type(self):
        return DataType.NUMERIC

    @property
    def value(self):
        return self.data_type.value


class ListType(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: TYPEDEF_TYPES

    @field_validator("type", mode="plain")
    def validate_type(cls, v):
        return v

    def __str__(self) -> str:
        return f"ListType<{self.type}>"

    @property
    def data_type(self):
        return DataType.LIST

    @property
    def value(self):
        return self.data_type.value

    @property
    def value_data_type(
        self,
    ) -> CONCRETE_TYPES:
        if isinstance(self.type, DataTyped):
            return self.type.output_datatype
        return self.type


class MapType(BaseModel):
    key_type: DataType
    value_type: TYPEDEF_TYPES

    @field_validator("value_type", mode="plain")
    def validate_type(cls, v):
        return v

    @property
    def data_type(self):
        return DataType.MAP

    @property
    def value(self):
        return self.data_type.value

    @property
    def value_data_type(
        self,
    ) -> CONCRETE_TYPES:
        if isinstance(self.value_type, DataTyped):
            return self.value_type.output_datatype
        return self.value_type

    @property
    def key_data_type(
        self,
    ) -> CONCRETE_TYPES:
        if isinstance(self.key_type, DataTyped):
            return self.key_type.output_datatype
        return self.key_type


class StructType(BaseModel):
    fields: Sequence[TYPEDEF_TYPES]
    fields_map: Dict[str, DataTyped | int | float | str]

    @field_validator("fields", mode="plain")
    def validate_type(cls, v):
        final = []
        for field in v:
            final.append(field)
        return final

    @field_validator("fields_map", mode="plain")
    def validate_fields_map(cls, v):
        return v

    @property
    def data_type(self):
        return DataType.STRUCT

    @property
    def value(self):
        return self.data_type.value


class ListWrapper(Generic[VT], UserList):
    """Used to distinguish parsed list objects from other lists"""

    def __init__(self, *args, type: DataType, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = type

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Callable[[Any], core_schema.CoreSchema]
    ) -> core_schema.CoreSchema:
        args = get_args(source_type)
        if args:
            schema = handler(List[args])  # type: ignore
        else:
            schema = handler(List)
        return core_schema.no_info_after_validator_function(cls.validate, schema)

    @classmethod
    def validate(cls, v):
        return cls(v, type=arg_to_datatype(v[0]))


class MapWrapper(Generic[KT, VT], UserDict):
    """Used to distinguish parsed map objects from other dicts"""

    def __init__(self, *args, key_type: DataType, value_type: DataType, **kwargs):
        super().__init__(*args, **kwargs)
        self.key_type = key_type
        self.value_type = value_type

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Callable[[Any], core_schema.CoreSchema]
    ) -> core_schema.CoreSchema:
        args = get_args(source_type)
        if args:
            schema = handler(Dict[args])  # type: ignore
        else:
            schema = handler(Dict)
        return core_schema.no_info_after_validator_function(cls.validate, schema)

    @classmethod
    def validate(cls, v):
        return cls(
            v,
            key_type=arg_to_datatype(list(v.keys()).pop()),
            value_type=arg_to_datatype(list(v.values()).pop()),
        )


class TupleWrapper(Generic[VT], tuple):
    """Used to distinguish parsed tuple objects from other tuples"""

    def __init__(self, val, type: DataType, **kwargs):
        super().__init__()
        self.type = type
        self.val = val

    def __getnewargs__(self):
        return (self.val, self.type)

    def __new__(cls, val, type: DataType, **kwargs):
        return super().__new__(cls, tuple(val))
        # self.type = type

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Callable[[Any], core_schema.CoreSchema]
    ) -> core_schema.CoreSchema:
        args = get_args(source_type)
        if args:
            schema = handler(Tuple[args])  # type: ignore
        else:
            schema = handler(Tuple)
        return core_schema.no_info_after_validator_function(cls.validate, schema)

    @classmethod
    def validate(cls, v):
        return cls(v, type=arg_to_datatype(v[0]))


def list_to_wrapper(args):
    types = [arg_to_datatype(arg) for arg in args]
    assert len(set(types)) == 1
    return ListWrapper(args, type=types[0])


def tuple_to_wrapper(args):
    types = [arg_to_datatype(arg) for arg in args]
    assert len(set(types)) == 1
    return TupleWrapper(args, type=types[0])


def dict_to_map_wrapper(arg):
    key_types = [arg_to_datatype(arg) for arg in arg.keys()]

    value_types = [arg_to_datatype(arg) for arg in arg.values()]
    assert len(set(key_types)) == 1
    assert len(set(key_types)) == 1
    return MapWrapper(arg, key_type=key_types[0], value_type=value_types[0])


def merge_datatypes(
    inputs: list[
        DataType | ListType | StructType | MapType | NumericType | TraitDataType
    ],
) -> DataType | ListType | StructType | MapType | NumericType | TraitDataType:
    """This is a temporary hack for doing between
    allowable datatype transformation matrix"""
    if len(inputs) == 1:
        return inputs[0]
    if set(inputs) == {DataType.INTEGER, DataType.FLOAT}:
        return DataType.FLOAT
    if set(inputs) == {DataType.INTEGER, DataType.NUMERIC}:
        return DataType.NUMERIC
    if any(isinstance(x, NumericType) for x in inputs) and all(
        isinstance(x, NumericType)
        or x in (DataType.INTEGER, DataType.FLOAT, DataType.NUMERIC)
        for x in inputs
    ):
        candidate = next(x for x in inputs if isinstance(x, NumericType))
        return candidate
    return inputs[0]


def is_compatible_datatype(left, right):
    # for unknown types, we can't make any assumptions
    if all(
        isinstance(x, NumericType)
        or x in (DataType.INTEGER, DataType.FLOAT, DataType.NUMERIC)
        for x in (left, right)
    ):
        return True
    elif isinstance(left, NumericType) or isinstance(right, NumericType):
        return False
    if right == DataType.UNKNOWN or left == DataType.UNKNOWN:
        return True
    if left == right:
        return True
    if {left, right} == {DataType.NUMERIC, DataType.FLOAT}:
        return True

    if {left, right} == {DataType.NUMERIC, DataType.INTEGER}:
        return True
    if {left, right} == {DataType.FLOAT, DataType.INTEGER}:
        return True
    return False


def arg_to_datatype(arg) -> CONCRETE_TYPES:

    if isinstance(arg, MagicConstants):
        if arg == MagicConstants.NULL:
            return DataType.NULL
        raise ValueError(f"Cannot parse arg datatype for arg of type {arg}")
    elif isinstance(arg, bool):
        return DataType.BOOL
    elif isinstance(arg, int):
        return DataType.INTEGER
    elif isinstance(arg, str):
        return DataType.STRING
    elif isinstance(arg, float):
        return DataType.FLOAT
    elif isinstance(arg, NumericType):
        return arg
    elif isinstance(arg, TraitDataType):
        return arg
    elif isinstance(arg, ListWrapper):
        return ListType(type=arg.type)
    elif isinstance(arg, DataTyped):
        return arg.output_datatype
    elif isinstance(arg, TupleWrapper):
        return ListType(type=arg.type)
    elif isinstance(arg, list):
        wrapper = list_to_wrapper(arg)
        return ListType(type=wrapper.type)
    elif isinstance(arg, MapWrapper):
        return MapType(key_type=arg.key_type, value_type=arg.value_type)
    elif isinstance(arg, datetime):
        return DataType.DATETIME
    elif isinstance(arg, date):
        return DataType.DATE
    else:
        raise ValueError(
            f"Cannot parse arg datatype for arg of raw type {type(arg)} value {arg}"
        )
