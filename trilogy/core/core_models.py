
from __future__ import annotations

import difflib
import hashlib
import os
from abc import ABC
from collections import UserDict, UserList, defaultdict, UserString
from datetime import date, datetime
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    Generic,
    ItemsView,
    List,
    Never,
    Optional,
    Self,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
    get_args,
        runtime_checkable,
    Protocol,
    TYPE_CHECKING
)

from lark.tree import Meta
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    computed_field,
    field_validator,
)
from pydantic.functional_validators import PlainValidator
from pydantic_core import core_schema

from trilogy.constants import (
    CONFIG,
    DEFAULT_NAMESPACE,
    ENV_CACHE_NAME,
    MagicConstants,
    logger,
)
from trilogy.core.constants import (
    ALL_ROWS_CONCEPT,
    CONSTANT_DATASET,
    INTERNAL_NAMESPACE,
    PERSISTED_CONCEPT_PREFIX,
)
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    DatePart,
    FunctionClass,
    FunctionType,
    Granularity,
    InfiniteFunctionArgs,
    IOType,
    JoinType,
    Modifier,
    Ordering,
    Purpose,
    PurposeLineage,
    SelectFiltering,
    ShowCategory,
    SourceType,
    WindowOrder,
    WindowType,
)
from trilogy.core.exceptions import (
    InvalidSyntaxException,
    UndefinedConceptException,
)
from trilogy.utility import unique


if TYPE_CHECKING:
    from trilogy.core.author_models import Environment, Concept
    from trilogy.core.execute_models import BoundEnvironment

KT = TypeVar("KT")
VT = TypeVar("VT")
LT = TypeVar("LT")

# Abstravt Base Classes
class Reference(ABC):

    def instantiate(self, environment: BoundEnvironment):
        raise NotImplementedError

class Namespaced(ABC):
    def with_namespace(self, namespace: str):
        raise NotImplementedError

class Addressable(ABC):

    @cached_property
    def address(self):
        raise NotImplemented
    
class Concrete(ABC):

    @property
    def reference(self):
        raise NotImplemented

def address_with_namespace(address: str, namespace: str) -> str:
    if address.split(".", 1)[0] == DEFAULT_NAMESPACE:
        return f"{namespace}.{address.split('.',1)[1]}"
    return f"{namespace}.{address}"

class RawColumnExpr(BaseModel):
    text: str

class ConceptRef(Namespaced, Reference, BaseModel):
    address: str
    line_no: int | None = None

    @classmethod
    def parse(cls, v):
        if isinstance(v, ConceptRef):
            return v
        elif isinstance(v, str):
            return ConceptRef(address=v)
        elif isinstance(v, Concrete):
            return v.reference
        else:
            raise ValueError(f"Invalid concept reference {v}")

    def __hash__(self):
        return hash(self.address)

    def __init__(self, **data):
        super().__init__(**data)

    def __eq__(self, other):
        if isinstance(other, str):
            return self.address == other
        return self.address == other.address

    @property
    def namespace(self) -> str:
        if "." in self.address:
            return self.address.rsplit(".", 1)[0]
        return DEFAULT_NAMESPACE

    @property
    def name(self) -> str:
        return self.address.split(".")[-1]

    def instantiate(self, environment: "Environment") -> "Concept":
        base = environment.concepts.__getitem__(self.address, self.line_no)
        if isinstance(base, Reference):
            base = base.instantiate(environment)
        return base

    def with_namespace(self, namespace: str) -> "ConceptRef":
        return ConceptRef(
            address=address_with_namespace(self.address, namespace),
            line_no=self.line_no,
        )


@runtime_checkable
class Typed(Protocol):
    datatype: DataType | ListType | StructType | MapType | NumericType
    purpose: Purpose
    granularity: Granularity

class TypedSentinal(BaseModel):
    # sentinal showing this implements the TypedProtocol
    pass


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


class NumericType(BaseModel):
    precision: int = 20
    scale: int = 5

    @property
    def data_type(self):
        return DataType.NUMERIC

    @property
    def value(self):
        return self.data_type.value


class ListType(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: ALL_TYPES

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
    ) -> DataType | StructType | MapType | ListType | NumericType:
        if isinstance(self.type, Typed):
            return self.type.datatype
        return self.type


class MapType(BaseModel):
    key_type: DataType
    value_type: ALL_TYPES

    @property
    def data_type(self):
        return DataType.MAP

    @property
    def value(self):
        return self.data_type.value

    @property
    def value_data_type(
        self,
    ) -> DataType | StructType | MapType | ListType | NumericType:
        if isinstance(self.value_type, Typed):
            return self.value_type.datatype
        return self.value_type

    @property
    def key_data_type(
        self,
    ) -> DataType | StructType | MapType | ListType | NumericType:
        if isinstance(self.key_type, Typed):
            return self.key_type.datatype
        return self.key_type


class StructType(BaseModel):
    fields: List[ALL_TYPES]
    fields_map: Dict[str, BaseModel |  int | float | str]

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
        base = super().__new__(cls, tuple(val))
        setattr(base, 'type', type)
        return base
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
        return cls(v, type=DataType.INTEGER)



class Metadata(BaseModel):
    """Metadata container object.
    TODO: support arbitrary tags"""

    description: Optional[str] = None
    line_number: Optional[int] = None
    concept_source: ConceptSource = ConceptSource.MANUAL



def is_compatible_datatype(left, right):
    # for unknown types, we can't make any assumptions
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


ALL_TYPES = Union[
    "DataType", "MapType", "ListType", "NumericType", "StructType", "ConceptRef"
]



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
    inputs: list[DataType | ListType | StructType | MapType | NumericType],
) -> DataType | ListType | StructType | MapType | NumericType:
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


def arg_to_datatype(arg) -> DataType | ListType | StructType | MapType | NumericType:
    if isinstance(arg, DataType):
        return arg
    elif isinstance(arg, TypedSentinal):
        return arg.datatype
    elif isinstance(arg, MagicConstants):
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
    elif isinstance(arg, ListWrapper):
        return ListType(type=arg.type)
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
        raise ValueError(f"Cannot parse arg datatype for arg of raw type {type(arg)}")



def arg_to_purpose(arg) -> Purpose:
    if isinstance(arg, (TypedSentinal,)):
        return arg.purpose
    # elif isinstance(arg, WindowItem):
    #     return Purpose.PROPERTY
    # elif isinstance(arg, BoundConcept):
    #     base = arg.purpose
    #     if (
    #         isinstance(arg.lineage, AggregateWrapper)
    #         and arg.lineage.by
    #         and base == Purpose.METRIC
    #     ):
    #         return Purpose.PROPERTY
    #     return arg.purpose
    elif isinstance(arg, (int, float, str, bool, list, NumericType, DataType)):
        return Purpose.CONSTANT
    elif isinstance(arg, DatePart):
        return Purpose.CONSTANT
    elif isinstance(arg, MagicConstants):
        return Purpose.CONSTANT
    else:
        raise ValueError(f"Cannot parse arg purpose for {arg} of type {type(arg)}")



def args_to_output_purpose(args) -> Purpose:
    has_metric = False
    has_non_constant = False
    has_non_single_row_constant = False
    if not args:
        return Purpose.CONSTANT
    for arg in args:
        purpose = arg_to_purpose(arg)
        if purpose == Purpose.METRIC:
            has_metric = True
        if purpose != Purpose.CONSTANT:
            has_non_constant = True
        if isinstance(arg, Typed) and arg.granularity != Granularity.SINGLE_ROW:
            has_non_single_row_constant = True
    if args and not has_non_constant and not has_non_single_row_constant:
        return Purpose.CONSTANT
    if has_metric:
        return Purpose.METRIC
    return Purpose.PROPERTY
