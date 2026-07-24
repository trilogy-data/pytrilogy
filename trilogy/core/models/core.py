from __future__ import annotations

import re
from collections import UserDict, UserList
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Generic,
    TypeVar,
    Union,
    get_args,
)

from pydantic_core import core_schema

from trilogy.constants import (
    MagicConstants,
)
from trilogy.core.enums import ComparisonOperator, DatePart, Modifier, Ordering


class DataTyped:
    output_datatype: CONCRETE_TYPES


class Addressable:

    @property
    def _address(self):
        return self.address


TYPEDEF_TYPES = Union[
    "DataType",
    "MapType",
    "ArrayType",
    "NumericType",
    "StructType",
    "DataTyped",
    "TraitDataType",
    "EnumType",
    "ValidatedType",
]

CONCRETE_TYPES = Union[
    "DataType",
    "MapType",
    "ArrayType",
    "NumericType",
    "StructType",
    "TraitDataType",
    "EnumType",
    "ValidatedType",
]

KT = TypeVar("KT")
VT = TypeVar("VT")
LT = TypeVar("LT")


class DataType(Enum):
    # PRIMITIVES
    STRING = "string"
    BYTES = "bytes"
    BOOL = "bool"
    MAP = "map"
    NUMBER = "number"
    FLOAT = "float"
    DOUBLE = "double"
    NUMERIC = "numeric"
    INTEGER = "int"
    BIGINT = "bigint"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    DATE_PART = "date_part"
    STRUCT = "struct"
    GEOGRAPHY = "geography"
    NULL = "null"

    # GRANULAR
    UNIX_SECONDS = "unix_seconds"

    # PARSING
    UNKNOWN = "unknown"
    ANY = "any"

    @property
    def data_type(self):
        return self

    def __str__(self) -> str:
        return self.name


@dataclass
class TraitDataType:
    type: CONCRETE_TYPES
    traits: list[str] = field(default_factory=list)

    def __hash__(self):
        return hash(self.type)

    def __str__(self) -> str:
        return f"Trait<{self.type}, {self.traits}>"

    def __eq__(self, other):
        if isinstance(other, DataType):
            return self.type == other
        elif isinstance(other, TraitDataType):
            return self.type == other.type and self.traits == other.traits
        elif isinstance(other, (EnumType, ValidatedType)):
            return self.type == other.type
        return False

    @property
    def data_type(self):
        return self.type

    @property
    def value(self):
        return self.data_type.value


@dataclass
class NumericType:
    precision: int = 20
    scale: int = 5

    def __str__(self) -> str:
        return f"Numeric({self.precision},{self.scale})"

    def __hash__(self):
        return hash((DataType.NUMERIC, self.precision, self.scale))

    @property
    def data_type(self):
        return DataType.NUMERIC

    @property
    def value(self):
        return self.data_type.value


@dataclass
class EnumType:
    type: DataType
    values: list[Any]

    def __hash__(self):
        return hash((self.type, tuple(self.values)))

    def __str__(self) -> str:
        return f"enum<{', '.join(repr(v) for v in self.values)}>"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DataType):
            return self.type == other
        if isinstance(other, EnumType):
            return self.type == other.type and self.values == other.values
        if isinstance(other, (TraitDataType, ValidatedType)):
            return self.type == other.type
        return False

    @property
    def data_type(self) -> DataType:
        return self.type

    @property
    def value(self) -> str:
        return self.type.value


RANGE_BOUND_TYPES = Union[int, float, date, datetime, None]


def _render_bound(value: RANGE_BOUND_TYPES) -> str:
    if isinstance(value, (date, datetime)):
        return f"'{value.isoformat()}'"
    return str(value)


@dataclass(frozen=True)
class ValueRange:
    min: RANGE_BOUND_TYPES = None
    max: RANGE_BOUND_TYPES = None

    def __post_init__(self):
        if self.min is None and self.max is None:
            raise ValueError("ValueRange requires at least one bound")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError(f"ValueRange min {self.min} exceeds max {self.max}")

    def __str__(self) -> str:
        if self.min is not None and self.min == self.max:
            return _render_bound(self.min)
        left = _render_bound(self.min) if self.min is not None else ""
        right = _render_bound(self.max) if self.max is not None else ""
        return f"{left}..{right}"

    def contains(self, value: Any) -> bool:
        try:
            if self.min is not None and value < self.min:
                return False
            if self.max is not None and value > self.max:
                return False
        except TypeError:
            return False
        return True


@dataclass
class ValidatedType:
    """A base type constrained by declared validators: inclusive value ranges
    (numeric/date/datetime bases) or a full-match regex (string base).
    Compares equal to its bare base type, mirroring EnumType/TraitDataType."""

    type: DataType | NumericType
    ranges: tuple[ValueRange, ...] = ()
    pattern: str | None = None

    def __hash__(self):
        return hash(self.type)

    def __str__(self) -> str:
        if isinstance(self.type, NumericType):
            base = f"numeric({self.type.precision},{self.type.scale})"
        else:
            base = self.type.value
        if self.pattern is not None:
            return f"{base}[{self.pattern!r}]"
        return f"{base}[{', '.join(str(r) for r in self.ranges)}]"

    def __eq__(self, other):
        if isinstance(other, DataType):
            return self.type == other
        if isinstance(other, ValidatedType):
            return (
                self.type == other.type
                and self.ranges == other.ranges
                and self.pattern == other.pattern
            )
        if isinstance(other, (TraitDataType, EnumType)):
            return self.type == other.type
        return False

    @property
    def data_type(self) -> DataType:
        if isinstance(self.type, NumericType):
            return DataType.NUMERIC
        return self.type

    @property
    def value(self) -> str:
        return self.data_type.value

    def check_value(self, value: Any) -> bool:
        """True when a non-null value satisfies the declared validators."""
        if self.pattern is not None:
            return (
                isinstance(value, str) and re.fullmatch(self.pattern, value) is not None
            )
        if not self.ranges:
            return True
        return any(r.contains(value) for r in self.ranges)


@dataclass(frozen=True)
class ArrayType:
    type: TYPEDEF_TYPES

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any):
        return core_schema.is_instance_schema(cls)

    def __hash__(self):
        return hash((DataType.ARRAY, self.type))

    def __str__(self) -> str:
        return f"ArrayType<{self.type}>"

    @property
    def data_type(self):
        return DataType.ARRAY

    @property
    def value(self) -> str:
        return self.data_type.value

    @property
    def value_data_type(
        self,
    ) -> CONCRETE_TYPES:
        if isinstance(self.type, DataTyped):
            return self.type.output_datatype
        return self.type


@dataclass
class MapType:
    key_type: TYPEDEF_TYPES
    value_type: TYPEDEF_TYPES

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any):
        return core_schema.is_instance_schema(cls)

    def __hash__(self):
        return hash((DataType.MAP, self.key_type, self.value_type))

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


@dataclass
class StructComponent:
    name: str
    type: TYPEDEF_TYPES
    modifiers: list[Modifier] = field(default_factory=list)

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any):
        return core_schema.is_instance_schema(cls)


@dataclass
class StructType:
    fields: Sequence[StructComponent | TYPEDEF_TYPES]
    fields_map: dict[str, DataTyped | int | float | str | StructComponent]

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any):
        return core_schema.is_instance_schema(cls)

    def __repr__(self):
        return "struct<{}>".format(
            ", ".join(
                f"{f.name}:{f.type!s}"
                for f in self.fields
                if isinstance(f, StructComponent)
            )
        )

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def data_type(self):
        return DataType.STRUCT

    @property
    def value(self):
        return self.data_type.value

    @property
    def field_types(self) -> dict[str, CONCRETE_TYPES]:
        out: dict[str, CONCRETE_TYPES] = {}
        keys = list(self.fields_map.keys())
        for idx, f in enumerate(self.fields):
            if isinstance(f, StructComponent):
                out[f.name] = arg_to_datatype(f.type)
            elif isinstance(f, DataTyped):
                out[keys[idx]] = f.output_datatype
            else:
                out[keys[idx]] = f
        return out

    def __hash__(self):
        return hash(str(self))


class ListWrapper(Generic[VT], UserList):
    """Used to distinguish parsed list objects from other lists"""

    def __init__(self, *args, type: DataType, nullable: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = type
        self.nullable = nullable

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Callable[[Any], core_schema.CoreSchema]
    ) -> core_schema.CoreSchema:
        args = get_args(source_type)
        if args:
            schema = handler(list[args])  # type: ignore
        else:
            schema = handler(list)
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
            schema = handler(dict[args])  # type: ignore
        else:
            schema = handler(dict)
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

    def __init__(self, val, type: CONCRETE_TYPES, nullable: bool = False, **kwargs):
        super().__init__()
        self.type = type
        self.val = val
        self.nullable = nullable

    def __getnewargs__(self):
        return (self.val, self.type)

    def __new__(cls, val, type: CONCRETE_TYPES, **kwargs):
        return super().__new__(cls, tuple(val))
        # self.type = type

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Callable[[Any], core_schema.CoreSchema]
    ) -> core_schema.CoreSchema:
        args = get_args(source_type)
        if args:
            schema = handler(tuple[args])  # type: ignore
        else:
            schema = handler(tuple)
        return core_schema.no_info_after_validator_function(cls.validate, schema)

    @classmethod
    def validate(cls, v):
        return cls(v, type=arg_to_datatype(v[0]))


def list_to_wrapper(args):
    rtypes = [arg_to_datatype(arg) for arg in args]
    types = [arg for arg in rtypes if arg != DataType.NULL]
    if not len(set(types)) == 1:
        raise SyntaxError(f"Cannot create a list with this set of types: {set(types)}")
    return ListWrapper(args, type=types[0], nullable=DataType.NULL in rtypes)


def tuple_to_wrapper(args):
    try:
        dtype, nullable = reduce_tuple_element_datatypes(
            [arg_to_datatype(arg) for arg in args]
        )
    except ValueError as e:
        raise SyntaxError(str(e))
    return TupleWrapper(args, type=dtype, nullable=nullable)


def dict_to_map_wrapper(arg):
    key_types = [arg_to_datatype(arg) for arg in arg.keys()]

    value_types = [arg_to_datatype(arg) for arg in arg.values()]
    assert len(set(key_types)) == 1
    assert len(set(key_types)) == 1
    return MapWrapper(arg, key_type=key_types[0], value_type=value_types[0])


def merge_datatypes(
    inputs: list[CONCRETE_TYPES],
) -> CONCRETE_TYPES:
    """This is a temporary hack for doing between
    allowable datatype transformation matrix"""
    if len(inputs) == 1:
        return inputs[0]

    # TraitDataType hashes/compares equal to its underlying DataType, so a
    # direct set(inputs) comparison silently discards trait-bearing inputs
    # and collapses them with their bare counterparts. Compare on underlying
    # types and prefer a trait-bearing input when picking the winner.
    def _base(x: CONCRETE_TYPES) -> CONCRETE_TYPES:
        return x.type if isinstance(x, TraitDataType) else x

    def _prefer_trait(data_type: DataType) -> CONCRETE_TYPES:
        for inp in inputs:
            if isinstance(inp, TraitDataType) and inp.type == data_type:
                return inp
        return data_type

    base_types = [_base(x) for x in inputs]
    base_set = set(base_types)

    if base_set == {DataType.INTEGER, DataType.FLOAT}:
        return _prefer_trait(DataType.FLOAT)
    # double is the wider float — it wins over int and 4-byte float.
    if DataType.DOUBLE in base_set and base_set <= {
        DataType.INTEGER,
        DataType.FLOAT,
        DataType.DOUBLE,
    }:
        return _prefer_trait(DataType.DOUBLE)
    if base_set == {DataType.INTEGER, DataType.NUMERIC}:
        return _prefer_trait(DataType.NUMERIC)
    if any(isinstance(x, NumericType) for x in inputs) and all(
        isinstance(x, NumericType)
        or _base(x)
        in (DataType.INTEGER, DataType.FLOAT, DataType.DOUBLE, DataType.NUMERIC)
        for x in inputs
    ):
        candidate = next(x for x in inputs if isinstance(x, NumericType))
        return candidate
    return inputs[0]


def is_compatible_datatype(left, right):
    # for unknown types, we can't make any assumptions
    if isinstance(left, list):
        return any(is_compatible_datatype(ltype, right) for ltype in left)
    if isinstance(right, list):
        return any(is_compatible_datatype(left, rtype) for rtype in right)
    # Traits are pure annotations layered on top of an underlying type — strip
    # them before doing structural compatibility so numeric(15,2)::usd matches
    # the bare numeric(15,2) it wraps.
    if isinstance(left, TraitDataType):
        return is_compatible_datatype(left.type, right)
    if isinstance(right, TraitDataType):
        return is_compatible_datatype(left, right.type)
    # An enum is a constrained set over a base type — compatible with that base
    # (and with another enum over a compatible base): enum<string> matches a bare
    # string, enum<int> matches int, etc.
    if isinstance(left, EnumType):
        return is_compatible_datatype(left.type, right)
    if isinstance(right, EnumType):
        return is_compatible_datatype(left, right.type)
    # A validated type is a constrained domain over a base type — structurally
    # compatible with that base; the constraint itself is enforced separately.
    if isinstance(left, ValidatedType):
        return is_compatible_datatype(left.type, right)
    if isinstance(right, ValidatedType):
        return is_compatible_datatype(left, right.type)
    if left == DataType.ANY or right == DataType.ANY:
        return True
    if left == DataType.NULL or right == DataType.NULL:
        return True
    if all(
        isinstance(x, NumericType)
        or x
        in (
            DataType.INTEGER,
            DataType.BIGINT,
            DataType.FLOAT,
            DataType.DOUBLE,
            DataType.NUMERIC,
            DataType.NUMBER,
        )
        for x in (left, right)
    ):
        # All numeric types are mutually comparable — every SQL backend
        # coerces across the integer/float/numeric family.
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


def _envelope_violation(
    domain: str,
    operator: ComparisonOperator,
    value: Any,
    global_min: Any,
    global_max: Any,
) -> str | None:
    try:
        if operator == ComparisonOperator.GT:
            if global_max is not None and global_max <= value:
                return f"declared domain {domain} has no value > {_render_bound(value)}"
        elif operator == ComparisonOperator.GTE:
            if global_max is not None and global_max < value:
                return (
                    f"declared domain {domain} has no value >= {_render_bound(value)}"
                )
        elif operator == ComparisonOperator.LT:
            if global_min is not None and global_min >= value:
                return f"declared domain {domain} has no value < {_render_bound(value)}"
        elif operator == ComparisonOperator.LTE:
            if global_min is not None and global_min > value:
                return (
                    f"declared domain {domain} has no value <= {_render_bound(value)}"
                )
    except TypeError:
        return None
    return None


def constant_domain_violation(
    expected: Any,
    operator: ComparisonOperator,
    value: Any,
) -> str | None:
    """A reason string when comparing a literal against a concept whose declared
    domain (ValidatedType ranges/regex or EnumType membership) makes the
    comparison provably never match; None when satisfiable or undecidable.

    Only provably-FALSE predicates are flagged. In-domain predicates — even
    ones matching every declared value — must parse untouched: they can carry
    load-bearing join null-rejection through nullable FK paths
    (evals/tpcds_agent/bug_q16_enum_tautology_drops_joined_null_rejection.md),
    and a narrower authored intent than the current domain snapshot is
    legitimate. Error messages must never advise removing a predicate."""
    while isinstance(expected, TraitDataType):
        expected = expected.type
    if isinstance(value, Enum):
        return None
    if isinstance(expected, EnumType):
        members = expected.values
        if not members:
            return None
        if operator in (ComparisonOperator.EQ, ComparisonOperator.IN):
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                return None
            if value not in members:
                return (
                    f"{value!r} can never match a declared value of {expected} — "
                    "fix the constant, or update the enum declaration if the "
                    "domain is stale"
                )
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            return None
        if not all(
            isinstance(m, (int, float)) and not isinstance(m, bool) for m in members
        ):
            return None
        return _envelope_violation(
            str(expected), operator, value, min(members), max(members)
        )
    if not isinstance(expected, ValidatedType):
        return None
    if expected.pattern is not None:
        if operator not in (ComparisonOperator.EQ, ComparisonOperator.IN):
            return None
        if not isinstance(value, str):
            return None
        if not expected.check_value(value):
            return f"{value!r} can never match declared pattern {expected}"
        return None
    if not expected.ranges:
        return None
    if expected.data_type in (DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP):
        if not isinstance(value, (date, datetime)):
            return None
    elif isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    if operator in (ComparisonOperator.EQ, ComparisonOperator.IN):
        if not expected.check_value(value):
            return f"{value!r} is outside declared domain {expected}"
        return None
    mins: list[Any] = [r.min for r in expected.ranges]
    maxes: list[Any] = [r.max for r in expected.ranges]
    global_min = None if any(m is None for m in mins) else min(mins)
    global_max = None if any(m is None for m in maxes) else max(maxes)
    return _envelope_violation(str(expected), operator, value, global_min, global_max)


def reduce_tuple_element_datatypes(
    datatypes: list[CONCRETE_TYPES],
) -> tuple[CONCRETE_TYPES, bool]:
    """Collapse a literal tuple's element datatypes to a single representative
    type. Elements need only be pairwise-compatible (numeric family, enum-over-
    base, trait-wrapped), not identical. Raises ValueError naming the offending
    pair when two elements are genuinely incompatible. Returns (type, nullable)."""
    nullable = any(d == DataType.NULL for d in datatypes)
    non_null = [d for d in datatypes if d != DataType.NULL]
    if not non_null:
        return DataType.NULL, nullable
    merged: CONCRETE_TYPES = non_null[0]
    for nxt in non_null[1:]:
        if not is_compatible_datatype(merged, nxt):
            raise ValueError(
                f"Tuple elements have incompatible types {merged} and {nxt}"
            )
        merged = merge_datatypes([merged, nxt])
    return merged, nullable


def arg_to_datatype(arg) -> CONCRETE_TYPES:
    match arg:
        # Exact value matching with MagicConstants
        case MagicConstants.NULL:
            return DataType.NULL
        case MagicConstants():
            raise ValueError(f"Cannot parse arg datatype for arg of type {arg}")

        # Note: bool must come before int because bool is a subclass of int
        case bool():
            return DataType.BOOL
        case Ordering():
            return DataType.STRING  # TODO: revisit
        case int():
            return DataType.INTEGER
        case str():
            return DataType.STRING
        case bytes() | bytearray() | memoryview():
            return DataType.BYTES
        case float():
            return DataType.FLOAT
        case Decimal():
            return DataType.NUMERIC

        # Direct returns for existing type definitions
        case (
            DataType()
            | NumericType()
            | TraitDataType()
            | ArrayType()
            | MapType()
            | EnumType()
            | ValidatedType()
            | StructType()
        ):
            return arg

        # Complex wrappers and recursive calls
        case ListWrapper(type=t) | TupleWrapper(type=t):
            return ArrayType(type=t)

        case list():
            wrapper = list_to_wrapper(arg)
            return ArrayType(type=wrapper.type)

        case MapWrapper(key_type=kt, value_type=vt):
            return MapType(key_type=kt, value_type=vt)

        case DataTyped(output_datatype=dt):
            return dt

        case datetime():
            return DataType.DATETIME
        case date():
            return DataType.DATE

        case StructComponent(type=t):
            return arg_to_datatype(t)

        case DatePart():
            return DataType.DATE_PART

        case _:
            raise ValueError(
                f"Cannot parse arg datatype for arg of raw type {type(arg)} value {arg}"
            )
