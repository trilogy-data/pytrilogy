from __future__ import annotations
import difflib
import os
from enum import Enum
from typing import (
    Dict,
    TypeVar,
    List,
    Optional,
    Union,
    Set,
    Any,
    Sequence,
    ValuesView,
    Callable,
    Annotated,
    get_args,
    Generic,
    Tuple,
    Type,
    ItemsView,
)
from pydantic_core import core_schema
from pydantic.functional_validators import PlainValidator
from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    field_validator,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    computed_field,
)
from lark.tree import Meta
from pathlib import Path
from trilogy.constants import (
    logger,
    DEFAULT_NAMESPACE,
    ENV_CACHE_NAME,
    MagicConstants,
    CONFIG,
)
from trilogy.core.constants import (
    ALL_ROWS_CONCEPT,
    INTERNAL_NAMESPACE,
    CONSTANT_DATASET,
)
from trilogy.core.enums import (
    InfiniteFunctionArgs,
    Purpose,
    JoinType,
    Ordering,
    Modifier,
    FunctionType,
    FunctionClass,
    BooleanOperator,
    ComparisonOperator,
    WindowOrder,
    PurposeLineage,
    SourceType,
    WindowType,
    ConceptSource,
    DatePart,
    ShowCategory,
    Granularity,
)
from trilogy.core.exceptions import UndefinedConceptException, InvalidSyntaxException
from trilogy.utility import unique
from collections import UserList
from functools import cached_property
from abc import ABC


LOGGER_PREFIX = "[MODELS]"

KT = TypeVar("KT")
VT = TypeVar("VT")
LT = TypeVar("LT")


def get_version():
    from trilogy import __version__

    return __version__


def get_concept_arguments(expr) -> List["Concept"]:
    output = []
    if isinstance(expr, Concept):
        output += [expr]

    elif isinstance(
        expr,
        (
            Comparison,
            Conditional,
            Function,
            Parenthetical,
            AggregateWrapper,
            CaseWhen,
            CaseElse,
        ),
    ):
        output += expr.concept_arguments
    return output


ALL_TYPES = Union["DataType", "MapType", "ListType", "StructType", "Concept"]

NAMESPACED_TYPES = Union[
    "WindowItem",
    "FilterItem",
    "Conditional",
    "Comparison",
    "Concept",
    "CaseWhen",
    "CaseElse",
    "Function",
    "AggregateWrapper",
    "Parenthetical",
]


class Namespaced(ABC):

    def with_namespace(self, namespace: str):
        raise NotImplementedError


class ConceptArgs(ABC):

    @property
    def concept_arguments(self) -> List["Concept"]:
        raise NotImplementedError

    @property
    def existence_arguments(self) -> list[tuple["Concept", ...]]:
        return []

    @property
    def row_arguments(self) -> List["Concept"]:
        return self.concept_arguments


class SelectGrain(ABC):
    def with_select_grain(self, grain: Grain):
        raise NotImplementedError


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

    # GRANULAR
    UNIX_SECONDS = "unix_seconds"

    # PARSING
    UNKNOWN = "unknown"

    @property
    def data_type(self):
        return self


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
    def value_data_type(self) -> DataType | StructType | MapType | ListType:
        if isinstance(self.type, Concept):
            return self.type.datatype
        return self.type


class MapType(BaseModel):
    key_type: DataType
    content_type: ALL_TYPES

    @property
    def data_type(self):
        return DataType.MAP

    @property
    def value(self):
        return self.data_type.value


class StructType(BaseModel):
    fields: List[ALL_TYPES]
    fields_map: Dict[str, Concept] = Field(default_factory=dict)

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


class Metadata(BaseModel):
    """Metadata container object.
    TODO: support arbitrary tags"""

    description: Optional[str] = None
    line_number: Optional[int] = None
    concept_source: ConceptSource = ConceptSource.MANUAL


def lineage_validator(
    v: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
) -> Union[Function, WindowItem, FilterItem, AggregateWrapper]:
    if v and not isinstance(v, (Function, WindowItem, FilterItem, AggregateWrapper)):
        raise ValueError(v)
    return v


def empty_grain() -> Grain:
    return Grain(components=[])


class Concept(Namespaced, SelectGrain, BaseModel):
    name: str
    datatype: DataType | ListType | StructType | MapType
    purpose: Purpose
    metadata: Optional[Metadata] = Field(
        default_factory=lambda: Metadata(description=None, line_number=None),
        validate_default=True,
    )
    lineage: Optional[
        Union[
            Function,
            WindowItem,
            FilterItem,
            AggregateWrapper,
            RowsetItem,
            MultiSelectStatement | MergeStatement,
        ]
    ] = None
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    keys: Optional[Tuple["Concept", ...]] = None
    grain: "Grain" = Field(default=None, validate_default=True)
    modifiers: Optional[List[Modifier]] = Field(default_factory=list)

    def __hash__(self):
        return hash(str(self))

    @field_validator("keys", mode="before")
    @classmethod
    def keys_validator(cls, v, info: ValidationInfo):
        if v is None:
            return v
        if not isinstance(v, (list, tuple)):
            raise ValueError(f"Keys must be a list or tuple, got {type(v)}")
        if isinstance(v, list):
            return tuple(v)
        return v

    @field_validator("namespace", mode="plain")
    @classmethod
    def namespace_validation(cls, v):
        return v or DEFAULT_NAMESPACE

    @field_validator("metadata")
    @classmethod
    def metadata_validation(cls, v):
        v = v or Metadata()
        return v

    @field_validator("purpose", mode="after")
    @classmethod
    def purpose_validation(cls, v):
        if v == Purpose.AUTO:
            raise ValueError("Cannot set purpose to AUTO")
        return v

    @field_validator("grain", mode="before")
    @classmethod
    def parse_grain(cls, v, info: ValidationInfo) -> Grain:
        # this is silly - rethink how we do grains
        values = info.data
        if not v and values.get("purpose", None) == Purpose.KEY:
            v = Grain(
                components=[
                    Concept(
                        namespace=values.get("namespace", DEFAULT_NAMESPACE),
                        name=values["name"],
                        datatype=values["datatype"],
                        purpose=values["purpose"],
                        grain=Grain(),
                    )
                ]
            )
        elif (
            "lineage" in values
            and isinstance(values["lineage"], AggregateWrapper)
            and values["lineage"].by
        ):
            v = Grain(components=values["lineage"].by)
        elif not v:
            v = Grain(components=[])
        elif isinstance(v, Concept):
            v = Grain(components=[v])
        if not v:
            raise SyntaxError(f"Invalid grain {v} for concept {values['name']}")
        return v

    def __eq__(self, other: object):
        if isinstance(other, str):
            if self.address == other:
                return True
        if not isinstance(other, Concept):
            return False
        return (
            self.name == other.name
            and self.datatype == other.datatype
            and self.purpose == other.purpose
            and self.namespace == other.namespace
            and self.grain == other.grain
            # and self.keys == other.keys
        )

    def __str__(self):
        grain = ",".join([str(c.address) for c in self.grain.components])
        return f"{self.namespace}.{self.name}<{grain}>"

    @cached_property
    def address(self) -> str:
        return f"{self.namespace}.{self.name}"

    @property
    def output(self) -> "Concept":
        return self

    @property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name.replace(".", "_")
        elif self.namespace:
            return f"{self.namespace.replace('.','_')}_{self.name.replace('.','_')}"
        return self.name.replace(".", "_")

    @property
    def grain_components(self) -> List["Concept"]:
        return self.grain.components_copy if self.grain else []

    def with_namespace(self, namespace: str) -> "Concept":
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage.with_namespace(namespace) if self.lineage else None,
            grain=(
                self.grain.with_namespace(namespace)
                if self.grain
                else Grain(components=[])
            ),
            namespace=(
                namespace + "." + self.namespace
                if self.namespace
                and self.namespace != DEFAULT_NAMESPACE
                and self.namespace != namespace
                else namespace
            ),
            keys=(
                tuple([x.with_namespace(namespace) for x in self.keys])
                if self.keys
                else None
            ),
            modifiers=self.modifiers,
        )

    def with_select_grain(self, grain: Optional["Grain"] = None) -> "Concept":
        if not all([isinstance(x, Concept) for x in self.keys or []]):
            raise ValueError(f"Invalid keys {self.keys} for concept {self.address}")
        new_grain = grain or self.grain
        new_lineage = self.lineage
        if isinstance(self.lineage, SelectGrain):
            new_lineage = self.lineage.with_select_grain(new_grain)
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=new_lineage,
            grain=new_grain,
            namespace=self.namespace,
            keys=self.keys,
            modifiers=self.modifiers,
        )

    def with_grain(self, grain: Optional["Grain"] = None) -> "Concept":
        if not all([isinstance(x, Concept) for x in self.keys or []]):
            raise ValueError(f"Invalid keys {self.keys} for concept {self.address}")
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain if grain else Grain(components=[]),
            namespace=self.namespace,
            keys=self.keys,
            modifiers=self.modifiers,
        )

    @cached_property
    def _with_default_grain(self) -> "Concept":
        if self.purpose == Purpose.KEY:
            # we need to make this abstract
            grain = Grain(components=[self.with_grain(Grain())], nested=True)
        elif self.purpose == Purpose.PROPERTY:
            components = []
            if self.keys:
                components = [*self.keys]
            if self.lineage:
                for item in self.lineage.arguments:
                    if isinstance(item, Concept):
                        if item.keys and not all(c in components for c in item.keys):
                            components += item.sources
                        else:
                            components += item.sources
            grain = Grain(components=components)
        elif self.purpose == Purpose.METRIC:
            grain = Grain()
        elif self.purpose == Purpose.CONSTANT:
            if self.derivation != PurposeLineage.CONSTANT:
                grain = Grain(components=[self.with_grain(Grain())], nested=True)
            else:
                grain = self.grain
        else:
            grain = self.grain  # type: ignore
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain,
            keys=self.keys,
            namespace=self.namespace,
            modifiers=self.modifiers,
        )

    def with_default_grain(self) -> "Concept":
        return self._with_default_grain

    @property
    def sources(self) -> List["Concept"]:
        if self.lineage:
            output: List[Concept] = []

            def get_sources(
                expr: Union[
                    Function,
                    WindowItem,
                    FilterItem,
                    AggregateWrapper,
                    RowsetItem,
                    MultiSelectStatement | MergeStatement,
                ],
                output: List[Concept],
            ):
                for item in expr.arguments:
                    if isinstance(item, Concept):
                        if item.address == self.address:
                            raise SyntaxError(
                                f"Concept {self.address} references itself"
                            )
                        output.append(item)
                        output += item.sources
                    elif isinstance(item, Function):
                        get_sources(item, output)

            get_sources(self.lineage, output)
            return output
        return []

    @property
    def concept_arguments(self) -> List[Concept]:
        return self.lineage.concept_arguments if self.lineage else []

    @property
    def input(self):
        return [self] + self.sources

    @property
    def derivation(self) -> PurposeLineage:
        if self.lineage and isinstance(self.lineage, WindowItem):
            return PurposeLineage.WINDOW
        elif self.lineage and isinstance(self.lineage, FilterItem):
            return PurposeLineage.FILTER
        elif self.lineage and isinstance(self.lineage, AggregateWrapper):
            return PurposeLineage.AGGREGATE
        elif self.lineage and isinstance(self.lineage, RowsetItem):
            return PurposeLineage.ROWSET
        elif self.lineage and isinstance(self.lineage, MultiSelectStatement):
            return PurposeLineage.MULTISELECT
        elif self.lineage and isinstance(self.lineage, MergeStatement):
            return PurposeLineage.MERGE
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return PurposeLineage.AGGREGATE
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator == FunctionType.UNNEST
        ):
            return PurposeLineage.UNNEST
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in FunctionClass.SINGLE_ROW.value
        ):
            return PurposeLineage.CONSTANT

        elif self.lineage and isinstance(self.lineage, Function):
            if not self.lineage.concept_arguments:
                return PurposeLineage.CONSTANT
            return PurposeLineage.BASIC
        elif self.purpose == Purpose.CONSTANT:
            return PurposeLineage.CONSTANT
        return PurposeLineage.ROOT

    @property
    def granularity(self) -> Granularity:
        """ "used to determine if concepts need to be included in grain
        calculations"""
        if self.derivation == PurposeLineage.CONSTANT:
            # constants are a single row
            return Granularity.SINGLE_ROW
        elif self.derivation == PurposeLineage.AGGREGATE:
            # if it's an aggregate grouped over all rows
            # there is only one row left and it's fine to cross_join
            if all([x.name == ALL_ROWS_CONCEPT for x in self.grain.components]):
                return Granularity.SINGLE_ROW
        elif self.namespace == INTERNAL_NAMESPACE and self.name == ALL_ROWS_CONCEPT:
            return Granularity.SINGLE_ROW
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator == FunctionType.UNNEST
        ):
            return Granularity.MULTI_ROW
        elif self.lineage and all(
            [
                x.granularity == Granularity.SINGLE_ROW
                for x in self.lineage.concept_arguments
            ]
        ):
            return Granularity.SINGLE_ROW
        return Granularity.MULTI_ROW


class Grain(BaseModel):
    nested: bool = False
    components: List[Concept] = Field(default_factory=list, validate_default=True)

    @field_validator("components")
    def component_validator(cls, v, info: ValidationInfo):
        values = info.data
        if not values.get("nested", False):
            v2: List[Concept] = unique(
                [safe_concept(c).with_default_grain() for c in v], "address"
            )
        else:
            v2 = unique(v, "address")
        final = []
        for sub in v2:
            if sub.purpose in (Purpose.PROPERTY, Purpose.METRIC) and sub.keys:
                if all([c in v2 for c in sub.keys]):
                    continue
            elif sub.derivation == PurposeLineage.MERGE and isinstance(
                sub.lineage, MergeStatement
            ):
                parents = sub.lineage.concepts
                if any([p in v2 for p in parents]):
                    continue
            final.append(sub)
        v2 = sorted(final, key=lambda x: x.name)
        return v2

    @property
    def components_copy(self) -> List[Concept]:
        return [*self.components]

    def __str__(self):
        if self.abstract:
            return (
                "Grain<Abstract" + ",".join([c.address for c in self.components]) + ">"
            )
        return "Grain<" + ",".join([c.address for c in self.components]) + ">"

    def with_namespace(self, namespace: str) -> "Grain":
        return Grain(
            components=[c.with_namespace(namespace) for c in self.components],
            nested=self.nested,
        )

    @property
    def abstract(self):
        return not self.components or all(
            [c.name == ALL_ROWS_CONCEPT for c in self.components]
        )

    @cached_property
    def set(self):
        return set([c.address for c in self.components_copy])

    def __eq__(self, other: object):
        if isinstance(other, list):
            return self.set == set([c.address for c in other])
        if not isinstance(other, Grain):
            return False
        return self.set == other.set

    def issubset(self, other: "Grain"):
        return self.set.issubset(other.set)

    def union(self, other: "Grain"):
        addresses = self.set.union(other.set)

        return Grain(
            components=[c for c in self.components if c.address in addresses]
            + [c for c in other.components if c.address in addresses]
        )

    def isdisjoint(self, other: "Grain"):
        return self.set.isdisjoint(other.set)

    def intersection(self, other: "Grain") -> "Grain":
        intersection = self.set.intersection(other.set)
        components = [i for i in self.components if i.address in intersection]
        return Grain(components=components)

    def __add__(self, other: "Grain") -> "Grain":
        components: List[Concept] = []
        for clist in [self.components_copy, other.components_copy]:
            for component in clist:
                if component.with_default_grain() in components:
                    continue
                components.append(component.with_default_grain())
        base_components = [c for c in components if c.purpose == Purpose.KEY]
        for c in components:
            if c.purpose == Purpose.PROPERTY and not any(
                [key in base_components for key in (c.keys or [])]
            ):
                base_components.append(c)
            elif (
                c.purpose == Purpose.CONSTANT
                and not c.derivation == PurposeLineage.CONSTANT
            ):
                base_components.append(c)
        return Grain(components=base_components)

    def __radd__(self, other) -> "Grain":
        if other == 0:
            return self
        else:
            return self.__add__(other)


class RawColumnExpr(BaseModel):
    text: str


class ColumnAssignment(BaseModel):
    alias: str | RawColumnExpr | Function
    concept: Concept
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    def with_namespace(self, namespace: str) -> "ColumnAssignment":
        return ColumnAssignment(
            alias=(
                self.alias.with_namespace(namespace)
                if isinstance(self.alias, Function)
                else self.alias
            ),
            concept=self.concept.with_namespace(namespace),
            modifiers=self.modifiers,
        )


class Statement(BaseModel):
    pass


class LooseConceptList(BaseModel):
    concepts: List[Concept]

    @cached_property
    def addresses(self) -> set[str]:
        return {s.address for s in self.concepts}

    @classmethod
    def validate(cls, v):
        return cls(v)

    def __str__(self) -> str:
        return f"lcl{str(self.addresses)}"

    def __iter__(self):
        return iter(self.concepts)

    def __eq__(self, other):
        if not isinstance(other, LooseConceptList):
            return False
        return self.addresses == other.addresses

    def issubset(self, other):
        if not isinstance(other, LooseConceptList):
            return False
        return self.addresses.issubset(other.addresses)

    def __contains__(self, other):
        if isinstance(other, str):
            return other in self.addresses
        if not isinstance(other, Concept):
            return False
        return other.address in self.addresses

    def difference(self, other):
        if not isinstance(other, LooseConceptList):
            return False
        return self.addresses.difference(other.addresses)

    def isdisjoint(self, other):
        if not isinstance(other, LooseConceptList):
            return False
        return self.addresses.isdisjoint(other.addresses)


class Function(Namespaced, SelectGrain, BaseModel):
    operator: FunctionType
    arg_count: int = Field(default=1)
    output_datatype: DataType | ListType | StructType | MapType
    output_purpose: Purpose
    valid_inputs: Optional[
        Union[
            Set[DataType | ListType | StructType],
            List[Set[DataType | ListType | StructType]],
        ]
    ] = None
    arguments: Sequence[
        Union[
            Concept,
            "AggregateWrapper",
            "Function",
            int,
            float,
            str,
            DataType,
            ListType,
            DatePart,
            "Parenthetical",
            CaseWhen,
            "CaseElse",
            list,
            ListWrapper[int],
            ListWrapper[str],
            ListWrapper[float],
        ]
    ]

    def __str__(self):
        return f'{self.operator.value}({",".join([str(a) for a in self.arguments])})'

    @property
    def datatype(self):
        return self.output_datatype

    def with_select_grain(self, grain: Grain) -> Function:
        return Function(
            operator=self.operator,
            arguments=[
                (
                    c.with_select_grain(grain)
                    if isinstance(
                        c,
                        SelectGrain,
                    )
                    else c
                )
                for c in self.arguments
            ],
            output_datatype=self.output_datatype,
            output_purpose=self.output_purpose,
            valid_inputs=self.valid_inputs,
            arg_count=self.arg_count,
        )

    @field_validator("arguments")
    @classmethod
    def parse_arguments(cls, v, info: ValidationInfo):
        from trilogy.parsing.exceptions import ParseError

        values = info.data
        arg_count = len(v)
        target_arg_count = values["arg_count"]
        operator_name = values["operator"].name
        # surface right error
        if "valid_inputs" not in values:
            return v
        valid_inputs = values["valid_inputs"]
        if not arg_count <= target_arg_count:
            if target_arg_count != InfiniteFunctionArgs:
                raise ParseError(
                    f"Incorrect argument count to {operator_name} function, expects"
                    f" {target_arg_count}, got {arg_count}"
                )
        # if all arguments can be any of the set type
        # turn this into an array for validation
        if isinstance(valid_inputs, set):
            valid_inputs = [valid_inputs for _ in v]
        elif not valid_inputs:
            return v
        for idx, arg in enumerate(v):
            if (
                isinstance(arg, Concept)
                and arg.datatype.data_type not in valid_inputs[idx]
            ):
                if arg.datatype != DataType.UNKNOWN:
                    raise TypeError(
                        f"Invalid input datatype {arg.datatype.data_type} passed into position {idx}"
                        f" for {operator_name} from concept {arg.name}, valid is {valid_inputs[idx]}"
                    )
            if (
                isinstance(arg, Function)
                and arg.output_datatype not in valid_inputs[idx]
            ):
                if arg.output_datatype != DataType.UNKNOWN:
                    raise TypeError(
                        f"Invalid input datatype {arg.output_datatype} passed into"
                        f" {operator_name} from function {arg.operator.name}"
                    )
            # check constants
            comparisons: List[Tuple[Type, DataType]] = [
                (str, DataType.STRING),
                (int, DataType.INTEGER),
                (float, DataType.FLOAT),
                (bool, DataType.BOOL),
                (DatePart, DataType.DATE_PART),
            ]
            for ptype, dtype in comparisons:
                if isinstance(arg, ptype) and dtype in valid_inputs[idx]:
                    # attempt to exit early to avoid checking all types
                    break
                elif isinstance(arg, ptype):
                    raise TypeError(
                        f"Invalid {dtype} constant passed into {operator_name} {arg}, expecting one of {valid_inputs[idx]}"
                    )
        return v

    def with_namespace(self, namespace: str) -> "Function":
        return Function(
            operator=self.operator,
            arguments=[
                (
                    c.with_namespace(namespace)
                    if isinstance(
                        c,
                        Namespaced,
                    )
                    else c
                )
                for c in self.arguments
            ],
            output_datatype=self.output_datatype,
            output_purpose=self.output_purpose,
            valid_inputs=self.valid_inputs,
            arg_count=self.arg_count,
        )

    @property
    def concept_arguments(self) -> List[Concept]:
        base = []
        for arg in self.arguments:
            base += get_concept_arguments(arg)
        return base

    @property
    def output_grain(self):
        # aggregates have an abstract grain
        base_grain = Grain(components=[])
        if self.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return base_grain
        # scalars have implicit grain of all arguments
        for input in self.concept_arguments:
            base_grain += input.grain
        return base_grain

    @property
    def output_keys(self) -> list[Concept]:
        # aggregates have an abstract grain
        components = []
        # scalars have implicit grain of all arguments
        for input in self.concept_arguments:
            if input.purpose == Purpose.KEY:
                components.append(input)
            elif input.keys:
                components += input.keys
        return list(set(components))


class ConceptTransform(Namespaced, BaseModel):
    function: Function | FilterItem | WindowItem | AggregateWrapper
    output: Concept
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def input(self) -> List[Concept]:
        return [v for v in self.function.arguments if isinstance(v, Concept)]

    def with_namespace(self, namespace: str) -> "ConceptTransform":
        return ConceptTransform(
            function=self.function.with_namespace(namespace),
            output=self.output.with_namespace(namespace),
            modifiers=self.modifiers,
        )


class Window(BaseModel):
    count: int
    window_order: WindowOrder

    def __str__(self):
        return f"Window<{self.window_order}>"


class WindowItemOver(BaseModel):
    contents: List[Concept]


class WindowItemOrder(BaseModel):
    contents: List["OrderItem"]


class WindowItem(Namespaced, SelectGrain, BaseModel):
    type: WindowType
    content: Concept
    order_by: List["OrderItem"]
    over: List["Concept"] = Field(default_factory=list)
    index: Optional[int] = None

    def with_namespace(self, namespace: str) -> "WindowItem":
        return WindowItem(
            type=self.type,
            content=self.content.with_namespace(namespace),
            over=[x.with_namespace(namespace) for x in self.over],
            order_by=[x.with_namespace(namespace) for x in self.order_by],
        )

    def with_select_grain(self, grain: Grain) -> "WindowItem":
        return WindowItem(
            type=self.type,
            content=self.content.with_select_grain(grain),
            over=[x.with_select_grain(grain) for x in self.over],
            order_by=[x.with_select_grain(grain) for x in self.order_by],
        )

    @property
    def concept_arguments(self) -> List[Concept]:
        return self.arguments

    @property
    def arguments(self) -> List[Concept]:
        output = [self.content]
        for order in self.order_by:
            output += [order.output]
        for item in self.over:
            output += [item]
        return output

    @property
    def output(self) -> Concept:
        if isinstance(self.content, ConceptTransform):
            return self.content.output
        return self.content

    @output.setter
    def output(self, value):
        if isinstance(self.content, ConceptTransform):
            self.content.output = value
        else:
            self.content = value

    @property
    def input(self) -> List[Concept]:
        base = self.content.input
        for v in self.order_by:
            base += v.input
        for c in self.over:
            base += c.input
        return base

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def output_purpose(self):
        return Purpose.PROPERTY


class FilterItem(Namespaced, SelectGrain, BaseModel):
    content: Concept
    where: "WhereClause"

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    def with_namespace(self, namespace: str) -> "FilterItem":
        return FilterItem(
            content=self.content.with_namespace(namespace),
            where=self.where.with_namespace(namespace),
        )

    def with_select_grain(self, grain: Grain) -> FilterItem:
        return FilterItem(
            content=self.content.with_select_grain(grain),
            where=self.where.with_select_grain(grain),
        )

    @property
    def arguments(self) -> List[Concept]:
        output = [self.content]
        output += self.where.input
        return output

    @property
    def output(self) -> Concept:
        if isinstance(self.content, ConceptTransform):
            return self.content.output
        return self.content

    @output.setter
    def output(self, value):
        if isinstance(self.content, ConceptTransform):
            self.content.output = value
        else:
            self.content = value

    @property
    def input(self) -> List[Concept]:
        base = self.content.input
        base += self.where.input
        return base

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def output_purpose(self):
        return self.content.purpose

    @property
    def concept_arguments(self):
        return [self.content] + self.where.concept_arguments


class SelectItem(Namespaced, BaseModel):
    content: Union[Concept, ConceptTransform]
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def output(self) -> Concept:
        if isinstance(self.content, ConceptTransform):
            return self.content.output
        elif isinstance(self.content, WindowItem):
            return self.content.output
        return self.content

    @property
    def input(self) -> List[Concept]:
        return self.content.input

    def with_namespace(self, namespace: str) -> "SelectItem":
        return SelectItem(
            content=self.content.with_namespace(namespace),
            modifiers=self.modifiers,
        )


class OrderItem(SelectGrain, Namespaced, BaseModel):
    expr: Concept
    order: Ordering

    def with_namespace(self, namespace: str) -> "OrderItem":
        return OrderItem(expr=self.expr.with_namespace(namespace), order=self.order)

    def with_select_grain(self, grain: Grain) -> "OrderItem":
        return OrderItem(expr=self.expr.with_grain(grain), order=self.order)

    @property
    def input(self):
        return self.expr.input

    @property
    def output(self):
        return self.expr.output


class OrderBy(Namespaced, BaseModel):
    items: List[OrderItem]

    def with_namespace(self, namespace: str) -> "OrderBy":
        return OrderBy(items=[x.with_namespace(namespace) for x in self.items])


class SelectStatement(Namespaced, BaseModel):
    selection: List[SelectItem]
    where_clause: Optional["WhereClause"] = None
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None

    def __str__(self):
        from trilogy.parsing.render import render_query

        return render_query(self)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        for nitem in self.selection:
            if not isinstance(nitem.content, Concept):
                continue
            if nitem.content.grain == Grain():
                if nitem.content.derivation == PurposeLineage.AGGREGATE:
                    nitem.content = nitem.content.with_grain(self.grain)

    @field_validator("selection", mode="before")
    @classmethod
    def selection_validation(cls, v):
        new = []
        for item in v:
            if isinstance(item, (Concept, ConceptTransform)):
                new.append(SelectItem(content=item))
            else:
                new.append(item)
        return new

    @property
    def input_components(self) -> List[Concept]:
        output = set()
        output_list = []
        for item in self.selection:
            for concept in item.input:
                if concept.name in output:
                    continue
                output.add(concept.name)
                output_list.append(concept)
        if self.where_clause:
            for concept in self.where_clause.input:
                if concept.name in output:
                    continue
                output.add(concept.name)
                output_list.append(concept)

        return output_list

    @property
    def output_components(self) -> List[Concept]:
        output = []
        for item in self.selection:
            if isinstance(item, Concept):
                output.append(item)
            else:
                output.append(item.output)
        return output

    @property
    def hidden_components(self) -> List[Concept]:
        output = []
        for item in self.selection:
            if isinstance(item, SelectItem) and Modifier.HIDDEN in item.modifiers:
                output.append(item.output)
        return output

    @property
    def all_components(self) -> List[Concept]:
        return (
            self.input_components + self.output_components + self.grain.components_copy
        )

    def to_datasource(
        self,
        namespace: str,
        identifier: str,
        address: Address,
        grain: Grain | None = None,
    ) -> Datasource:
        columns = [
            # TODO: replace hardcoded replacement here
            ColumnAssignment(alias=c.address.replace(".", "_"), concept=c)
            for c in self.output_components
        ]
        new_datasource = Datasource(
            identifier=identifier,
            address=address,
            grain=grain or self.grain,
            columns=columns,
            namespace=namespace,
        )
        for column in columns:
            column.concept = column.concept.with_grain(new_datasource.grain)
        return new_datasource

    @property
    def grain(self) -> "Grain":
        output = []
        for item in self.output_components:
            if item.purpose == Purpose.KEY:
                output.append(item)
        if self.where_clause:
            for item in self.where_clause.concept_arguments:
                if item.purpose == Purpose.KEY:
                    output.append(item)
                # elif item.purpose == Purpose.PROPERTY and item.grain:
                #     output += item.grain.components
            # TODO: handle other grain cases
            # new if block by design
        # add back any purpose that is not at the grain
        # if a query already has the key of the property in the grain
        # we want to group to that grain and ignore the property, which is a derivation
        # otherwise, we need to include property as the group by
        for item in self.output_components:
            if (
                item.purpose == Purpose.PROPERTY
                and item.grain
                and (
                    not item.grain.components
                    or not item.grain.issubset(
                        Grain(components=unique(output, "address"))
                    )
                )
            ):
                output.append(item)
            if (
                item.purpose == Purpose.CONSTANT
                and item.derivation != PurposeLineage.CONSTANT
                and item.grain
                and (
                    not item.grain.components
                    or not item.grain.issubset(
                        Grain(components=unique(output, "address"))
                    )
                )
            ):
                output.append(item)
        return Grain(components=unique(output, "address"))

    def with_namespace(self, namespace: str) -> "SelectStatement":
        return SelectStatement(
            selection=[c.with_namespace(namespace) for c in self.selection],
            where_clause=(
                self.where_clause.with_namespace(namespace)
                if self.where_clause
                else None
            ),
            order_by=self.order_by.with_namespace(namespace) if self.order_by else None,
            limit=self.limit,
        )


class AlignItem(Namespaced, BaseModel):
    alias: str
    concepts: List[Concept]
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)

    @computed_field  # type: ignore
    @cached_property
    def concepts_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.concepts)

    def with_namespace(self, namespace: str) -> "AlignItem":
        return AlignItem(
            alias=self.alias,
            concepts=[c.with_namespace(namespace) for c in self.concepts],
            namespace=namespace,
        )

    def gen_concept(self, parent: MultiSelectStatement):
        datatypes = set([c.datatype for c in self.concepts])
        purposes = set([c.purpose for c in self.concepts])
        if len(datatypes) > 1:
            raise InvalidSyntaxException(
                f"Datatypes do not align for merged statements {self.alias}, have {datatypes}"
            )
        if len(purposes) > 1:
            purpose = Purpose.KEY
        else:
            purpose = list(purposes)[0]
        new = Concept(
            name=self.alias,
            datatype=datatypes.pop(),
            purpose=purpose,
            lineage=parent,
            namespace=parent.namespace,
        )
        return new


class AlignClause(Namespaced, BaseModel):
    items: List[AlignItem]

    def with_namespace(self, namespace: str) -> "AlignClause":
        return AlignClause(items=[x.with_namespace(namespace) for x in self.items])


class MultiSelectStatement(Namespaced, BaseModel):
    selects: List[SelectStatement]
    align: AlignClause
    namespace: str
    where_clause: Optional["WhereClause"] = None
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None

    def __repr__(self):
        return "MultiSelect<" + " MERGE ".join([str(s) for s in self.selects]) + ">"

    @computed_field  # type: ignore
    @cached_property
    def arguments(self) -> List[Concept]:
        output = []
        for select in self.selects:
            output += select.input_components
        return unique(output, "address")

    @computed_field  # type: ignore
    @cached_property
    def concept_arguments(self) -> List[Concept]:
        output = []
        for select in self.selects:
            output += select.input_components
        if self.where_clause:
            output += self.where_clause.concept_arguments
        return unique(output, "address")

    def get_merge_concept(self, check: Concept):
        for item in self.align.items:
            if check in item.concepts_lcl:
                return item.gen_concept(self)
        return None

    def with_namespace(self, namespace: str) -> "MultiSelectStatement":
        return MultiSelectStatement(
            selects=[c.with_namespace(namespace) for c in self.selects],
            align=self.align.with_namespace(namespace),
            namespace=namespace,
        )

    @property
    def grain(self):
        base = Grain()
        for select in self.selects:
            base += select.grain
        return base

    @computed_field  # type: ignore
    @cached_property
    def derived_concepts(self) -> List[Concept]:
        output = []
        for item in self.align.items:
            output.append(item.gen_concept(self))
        return output

    def find_source(self, concept: Concept, cte: CTE) -> Concept:
        for x in self.align.items:
            if concept.name == x.alias:
                for c in x.concepts:
                    if c.address in cte.output_lcl:
                        return c
        raise SyntaxError(
            f"Could not find upstream map for multiselect {str(concept)} on cte ({cte})"
        )

    @property
    def output_components(self) -> List[Concept]:
        output = self.derived_concepts
        for select in self.selects:
            output += select.output_components
        return unique(output, "address")

    @computed_field  # type: ignore
    @cached_property
    def hidden_components(self) -> List[Concept]:
        output = []
        for select in self.selects:
            output += select.hidden_components
        return output


class Address(BaseModel):
    location: str
    is_query: bool = False


class Query(BaseModel):
    text: str


def safe_concept(v: Union[Dict, Concept]) -> Concept:
    if isinstance(v, dict):
        return Concept.model_validate(v)
    return v


class GrainWindow(BaseModel):
    window: Window
    sort_concepts: List[Concept]

    def __str__(self):
        return (
            "GrainWindow<"
            + ",".join([c.address for c in self.sort_concepts])
            + f":{str(self.window)}>"
        )


def safe_grain(v) -> Grain:
    if isinstance(v, dict):
        return Grain.model_validate(v)
    elif isinstance(v, Grain):
        return v
    elif not v:
        return Grain(components=[])
    else:
        raise ValueError(f"Invalid input type to safe_grain {type(v)}")


class DatasourceMetadata(BaseModel):
    freshness_concept: Concept | None
    partition_fields: List[Concept] = Field(default_factory=list)


class MergeStatement(Namespaced, BaseModel):
    concepts: List[Concept]
    datatype: DataType | ListType | StructType | MapType

    @cached_property
    def concepts_lcl(self):
        return LooseConceptList(concepts=self.concepts)

    @property
    def merge_concept(self) -> Concept:
        bridge_name = "_".join([c.safe_address for c in self.concepts])
        return Concept(
            name=f"__merge_{bridge_name}",
            datatype=self.datatype,
            purpose=Purpose.PROPERTY,
            lineage=self,
            keys=tuple(self.concepts),
        )

    @property
    def arguments(self) -> List[Concept]:
        return self.concepts

    @property
    def concept_arguments(self) -> List[Concept]:
        return self.concepts

    def find_source(self, concept: Concept, cte: CTE) -> Concept:
        for x in self.concepts:
            for z in cte.output_columns:
                if z.address == x.address:
                    return z
        raise SyntaxError(
            f"Could not find upstream map for multiselect {str(concept)} on cte ({cte.name})"
        )

    def with_namespace(self, namespace: str) -> "MergeStatement":
        return MergeStatement(
            concepts=[c.with_namespace(namespace) for c in self.concepts],
            datatype=self.datatype,
        )


class Datasource(Namespaced, BaseModel):
    identifier: str
    columns: List[ColumnAssignment]
    address: Union[Address, str]
    grain: Grain = Field(
        default_factory=lambda: Grain(components=[]), validate_default=True
    )
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    metadata: DatasourceMetadata = Field(
        default_factory=lambda: DatasourceMetadata(freshness_concept=None)
    )

    @property
    def condition(self):
        return None

    @cached_property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_concepts)

    @property
    def can_be_inlined(self) -> bool:
        if isinstance(self.address, Address) and self.address.is_query:
            return False
        for x in self.columns:
            if not isinstance(x.alias, str):
                return False
        return True

    @property
    def non_partial_concept_addresses(self) -> set[str]:
        return set([c.address for c in self.full_concepts])

    @field_validator("namespace", mode="plain")
    @classmethod
    def namespace_validation(cls, v):
        return v or DEFAULT_NAMESPACE

    @field_validator("address")
    @classmethod
    def address_enforcement(cls, v):
        if isinstance(v, str):
            v = Address(location=v)
        return v

    @field_validator("grain", mode="plain")
    @classmethod
    def grain_enforcement(cls, v: Grain, info: ValidationInfo):
        values = info.data
        grain: Grain = safe_grain(v)
        if not grain.components:
            columns: List[ColumnAssignment] = values.get("columns", [])
            grain = Grain(
                components=[
                    c.concept.with_grain(Grain())
                    for c in columns
                    if c.concept.purpose == Purpose.KEY
                ]
            )
        return grain

    def add_column(self, concept: Concept, alias: str, modifiers=None):
        self.columns.append(
            ColumnAssignment(alias=alias, concept=concept, modifiers=modifiers)
        )
        # force refresh
        del self.output_lcl

    def __add__(self, other):
        if not other == self:
            raise ValueError(
                "Attempted to add two datasources that are not identical, this is not a valid operation"
            )
        return self

    def __repr__(self):
        return f"Datasource<{self.namespace}.{self.identifier}@<{self.grain}>"

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return (self.namespace + self.identifier).__hash__()

    def with_namespace(self, namespace: str):
        new_namespace = (
            namespace + "." + self.namespace
            if self.namespace and self.namespace != DEFAULT_NAMESPACE
            else namespace
        )
        return Datasource(
            identifier=self.identifier,
            namespace=new_namespace,
            grain=self.grain.with_namespace(namespace),
            address=self.address,
            columns=[c.with_namespace(namespace) for c in self.columns],
        )

    @cached_property
    def concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns]

    @property
    def group_required(self):
        return False

    @property
    def full_concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL not in c.modifiers]

    @property
    def output_concepts(self) -> List[Concept]:
        return self.concepts

    @property
    def partial_concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL in c.modifiers]

    def get_alias(
        self, concept: Concept, use_raw_name: bool = True, force_alias: bool = False
    ) -> Optional[str | RawColumnExpr] | Function:
        # 2022-01-22
        # this logic needs to be refined.
        # if concept.lineage:
        # #     return None
        for x in self.columns:
            if x.concept == concept or x.concept.with_grain(concept.grain) == concept:
                if use_raw_name:
                    return x.alias
                return concept.safe_address
        existing = [str(c.concept.with_grain(self.grain)) for c in self.columns]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {concept} not found on {self.identifier}; have"
            f" {existing}."
        )

    @property
    def name(self) -> str:
        return self.identifier
        # TODO: namespace all references
        # return f'{self.namespace}_{self.identifier}'

    @property
    def full_name(self) -> str:
        if not self.namespace:
            return self.identifier
        namespace = self.namespace.replace(".", "_") if self.namespace else ""
        return f"{namespace}_{self.identifier}"

    @property
    def safe_location(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address


class UnnestJoin(BaseModel):
    concept: Concept
    alias: str = "unnest"

    def __hash__(self):
        return (self.alias + self.concept.address).__hash__()


class InstantiatedUnnestJoin(BaseModel):
    concept: Concept
    alias: str = "unnest"


class BaseJoin(BaseModel):
    left_datasource: Union[Datasource, "QueryDatasource"]
    right_datasource: Union[Datasource, "QueryDatasource"]
    concepts: List[Concept]
    join_type: JoinType
    filter_to_mutual: bool = False

    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.left_datasource.full_name == self.right_datasource.full_name:
            raise SyntaxError(
                f"Cannot join a dataself to itself, joining {self.left_datasource} and"
                f" {self.right_datasource}"
            )
        final_concepts = []
        for concept in self.concepts:
            include = True
            for ds in [self.left_datasource, self.right_datasource]:
                if concept.address not in [c.address for c in ds.output_concepts]:
                    if self.filter_to_mutual:
                        include = False
                    else:
                        raise SyntaxError(
                            f"Invalid join, missing {concept} on {ds.name}, have"
                            f" {[c.address for c in ds.output_concepts]}"
                        )
            if include:
                final_concepts.append(concept)
        if not final_concepts and self.concepts:
            # if one datasource only has constants
            # we can join on 1=1
            for ds in [self.left_datasource, self.right_datasource]:
                # single rows
                if all(
                    [
                        c.granularity == Granularity.SINGLE_ROW
                        for c in ds.output_concepts
                    ]
                ):
                    self.concepts = []
                    return
                # if everything is at abstract grain, we can skip joins
                if all([c.grain == Grain() for c in ds.output_concepts]):
                    self.concepts = []
                    return

            left_keys = [c.address for c in self.left_datasource.output_concepts]
            right_keys = [c.address for c in self.right_datasource.output_concepts]
            match_concepts = [c.address for c in self.concepts]
            raise SyntaxError(
                "No mutual join keys found between"
                f" {self.left_datasource.identifier} and"
                f" {self.right_datasource.identifier}, left_keys {left_keys},"
                f" right_keys {right_keys},"
                f" provided join concepts {match_concepts}"
            )
        self.concepts = final_concepts

    @property
    def unique_id(self) -> str:
        # TODO: include join type?
        return (
            self.left_datasource.name
            + self.right_datasource.name
            + self.join_type.value
        )

    def __str__(self):
        return (
            f"{self.join_type.value} JOIN {self.left_datasource.identifier} and"
            f" {self.right_datasource.identifier} on"
            f" {','.join([str(k) for k in self.concepts])}"
        )


class QueryDatasource(BaseModel):
    input_concepts: List[Concept]
    output_concepts: List[Concept]
    source_map: Dict[str, Set[Union[Datasource, "QueryDatasource", "UnnestJoin"]]]

    datasources: List[Union[Datasource, "QueryDatasource"]]
    grain: Grain
    joins: List[BaseJoin | UnnestJoin]
    limit: Optional[int] = None
    condition: Optional[Union["Conditional", "Comparison", "Parenthetical"]] = Field(
        default=None
    )
    filter_concepts: List[Concept] = Field(default_factory=list)
    source_type: SourceType = SourceType.SELECT
    partial_concepts: List[Concept] = Field(default_factory=list)
    join_derived_concepts: List[Concept] = Field(default_factory=list)
    hidden_concepts: List[Concept] = Field(default_factory=list)
    force_group: bool | None = None
    existence_source_map: Dict[str, Set[Union[Datasource, "QueryDatasource"]]] = Field(
        default_factory=dict
    )

    def __repr__(self):
        return f"{self.identifier}@<{self.grain}>"

    @property
    def non_partial_concept_addresses(self) -> List[str]:
        return [
            c.address
            for c in self.output_concepts
            if c.address not in [z.address for z in self.partial_concepts]
        ]

    @field_validator("joins")
    @classmethod
    def validate_joins(cls, v):
        for join in v:
            if not isinstance(join, BaseJoin):
                continue
            if join.left_datasource.identifier == join.right_datasource.identifier:
                raise SyntaxError(
                    f"Cannot join a datasource to itself, joining {join.left_datasource}"
                )
        return v

    @field_validator("input_concepts")
    @classmethod
    def validate_inputs(cls, v):
        return unique(v, "address")

    @field_validator("output_concepts")
    @classmethod
    def validate_outputs(cls, v):
        return unique(v, "address")

    @field_validator("source_map")
    @classmethod
    def validate_source_map(cls, v, info: ValidationInfo):
        values = info.data
        expected = {c.address for c in values["output_concepts"]}.union(
            c.address for c in values["input_concepts"]
        )
        seen = set()
        for k, _ in v.items():
            seen.add(k)
        for x in expected:
            if x not in seen and CONFIG.validate_missing:
                raise SyntaxError(
                    f"source map missing {x} on (expected {expected}, have {seen})"
                )
        return v

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return (self.identifier).__hash__()

    @property
    def concepts(self):
        return self.output_concepts

    @property
    def name(self):
        return self.identifier

    @property
    def full_name(self):
        return self.identifier

    @property
    def group_required(self) -> bool:
        if self.force_group is True:
            return True
        if self.force_group is False:
            return False
        if self.source_type:
            if self.source_type in [
                SourceType.FILTER,
            ]:
                return False
            elif self.source_type in [
                SourceType.GROUP,
            ]:
                return True
        return False

    def __add__(self, other):
        # these are syntax errors to avoid being caught by current
        if not isinstance(other, QueryDatasource):
            raise SyntaxError("Can only merge two query datasources")
        if not other.grain == self.grain:
            raise SyntaxError(
                "Can only merge two query datasources with identical grain"
            )
        if not self.source_type == other.source_type:
            raise SyntaxError(
                "Can only merge two query datasources with identical source type"
            )
        if not self.group_required == other.group_required:
            raise SyntaxError(
                "can only merge two datasources if the group required flag is the same"
            )
        if not self.join_derived_concepts == other.join_derived_concepts:
            raise SyntaxError(
                "can only merge two datasources if the join derived concepts are the same"
            )
        if not self.force_group == other.force_group:
            raise SyntaxError(
                "can only merge two datasources if the force_group flag is the same"
            )
        logger.debug(
            f"{LOGGER_PREFIX} merging {self.name} with"
            f" {[c.address for c in self.output_concepts]} concepts and"
            f" {other.name} with {[c.address for c in other.output_concepts]} concepts"
        )

        merged_datasources = {}
        for ds in [*self.datasources, *other.datasources]:
            if ds.full_name in merged_datasources:
                merged_datasources[ds.full_name] = merged_datasources[ds.full_name] + ds
            else:
                merged_datasources[ds.full_name] = ds
        qds = QueryDatasource(
            input_concepts=unique(
                self.input_concepts + other.input_concepts, "address"
            ),
            output_concepts=unique(
                self.output_concepts + other.output_concepts, "address"
            ),
            source_map={**self.source_map, **other.source_map},
            datasources=list(merged_datasources.values()),
            grain=self.grain,
            joins=unique(self.joins + other.joins, "unique_id"),
            # joins = self.joins,
            condition=(
                self.condition + other.condition
                if (self.condition or other.condition)
                else None
            ),
            source_type=self.source_type,
            partial_concepts=unique(
                self.partial_concepts + other.partial_concepts, "address"
            ),
            join_derived_concepts=self.join_derived_concepts,
            force_group=self.force_group,
            hidden_concepts=unique(
                self.hidden_concepts + other.hidden_concepts, "address"
            ),
        )

        return qds

    @property
    def identifier(self) -> str:
        filters = abs(hash(str(self.condition))) if self.condition else ""
        grain = "_".join(
            [str(c.address).replace(".", "_") for c in self.grain.components]
        )
        # partial = "_".join([str(c.address).replace(".", "_") for c in self.partial_concepts])
        return (
            "_join_".join([d.name for d in self.datasources])
            + (f"_at_{grain}" if grain else "_at_abstract")
            + (f"_filtered_by_{filters}" if filters else "")
            # + (f"_partial_{partial}" if partial else "")
        )

    def get_alias(
        self,
        concept: Concept,
        use_raw_name: bool = False,
        force_alias: bool = False,
        source: str | None = None,
    ):
        for x in self.datasources:
            # query datasources should be referenced by their alias, always
            force_alias = isinstance(x, QueryDatasource)
            use_raw_name = isinstance(x, Datasource) and not force_alias
            if source and x.identifier != source:
                continue
            try:
                return x.get_alias(
                    concept.with_grain(self.grain),
                    use_raw_name,
                    force_alias=force_alias,
                )
            except ValueError as e:
                from trilogy.constants import logger

                logger.debug(e)
                continue
        existing = [c.with_grain(self.grain) for c in self.output_concepts]
        if concept in existing:
            return concept.name

        existing_str = [str(c) for c in existing]
        datasources = [ds.identifier for ds in self.datasources]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {str(concept)} not found on {self.identifier};"
            f" have {existing_str} from {datasources}."
        )

    @property
    def safe_location(self):
        return self.identifier


class Comment(BaseModel):
    text: str


class CTE(BaseModel):
    name: str
    source: "QueryDatasource"
    output_columns: List[Concept]
    source_map: Dict[str, list[str]]
    grain: Grain
    base: bool = False
    group_to_grain: bool = False
    existence_source_map: Dict[str, list[str]] = Field(default_factory=dict)
    parent_ctes: List["CTE"] = Field(default_factory=list)
    joins: List[Union["Join", "InstantiatedUnnestJoin"]] = Field(default_factory=list)
    condition: Optional[Union["Conditional", "Comparison", "Parenthetical"]] = None
    partial_concepts: List[Concept] = Field(default_factory=list)
    join_derived_concepts: List[Concept] = Field(default_factory=list)
    hidden_concepts: List[Concept] = Field(default_factory=list)
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    requires_nesting: bool = True
    base_name_override: Optional[str] = None
    base_alias_override: Optional[str] = None

    @computed_field  # type: ignore
    @property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_columns)

    @field_validator("output_columns")
    def validate_output_columns(cls, v):
        return unique(v, "address")

    def inline_parent_datasource(self, parent: CTE, force_group: bool = False) -> bool:
        qds_being_inlined = parent.source
        ds_being_inlined = qds_being_inlined.datasources[0]
        if not isinstance(ds_being_inlined, Datasource):
            return False
        if any(
            [
                x.identifier == ds_being_inlined.identifier
                for x in self.source.datasources
            ]
        ):
            return False

        self.source.datasources = [
            ds_being_inlined,
            *[
                x
                for x in self.source.datasources
                if x.identifier != qds_being_inlined.identifier
            ],
        ]
        # need to identify this before updating joins
        if self.base_name == parent.name:
            self.base_name_override = ds_being_inlined.safe_location
            self.base_alias_override = ds_being_inlined.identifier

        for join in self.joins:
            if isinstance(join, InstantiatedUnnestJoin):
                continue
            if join.left_cte.name == parent.name:
                join.left_cte = ds_being_inlined
            if join.right_cte.name == parent.name:
                join.right_cte = ds_being_inlined
        for k, v in self.source_map.items():
            if isinstance(v, list):
                self.source_map[k] = [
                    ds_being_inlined.name if x == parent.name else x for x in v
                ]
            elif v == parent.name:
                self.source_map[k] = ds_being_inlined.name
        self.parent_ctes = [x for x in self.parent_ctes if x.name != parent.name]
        if force_group:
            self.group_to_grain = True
        return True

    def __add__(self, other: "CTE"):
        logger.info('Merging two copies of CTE "%s"', self.name)
        if not self.grain == other.grain:
            error = (
                "Attempting to merge two ctes of different grains"
                f" {self.name} {other.name} grains {self.grain} {other.grain}| {self.group_to_grain} {other.group_to_grain}| {self.output_lcl} {other.output_lcl}"
            )
            raise ValueError(error)
        if not self.condition == other.condition:
            error = (
                "Attempting to merge two ctes with different conditions"
                f" {self.name} {other.name} conditions {self.condition} {other.condition}"
            )
            raise ValueError(error)
        self.partial_concepts = unique(
            self.partial_concepts + other.partial_concepts, "address"
        )
        self.parent_ctes = merge_ctes(self.parent_ctes + other.parent_ctes)

        self.source_map = {**self.source_map, **other.source_map}

        self.output_columns = unique(
            self.output_columns + other.output_columns, "address"
        )
        self.joins = unique(self.joins + other.joins, "unique_id")
        self.partial_concepts = unique(
            self.partial_concepts + other.partial_concepts, "address"
        )
        self.join_derived_concepts = unique(
            self.join_derived_concepts + other.join_derived_concepts, "address"
        )

        self.source.source_map = {**self.source.source_map, **other.source.source_map}
        self.source.output_concepts = unique(
            self.source.output_concepts + other.source.output_concepts, "address"
        )
        self.hidden_concepts = unique(
            self.hidden_concepts + other.hidden_concepts, "address"
        )
        return self

    @property
    def relevant_base_ctes(self):
        return self.parent_ctes

    @property
    def is_root_datasource(self) -> bool:
        return (
            len(self.source.datasources) == 1
            and isinstance(self.source.datasources[0], Datasource)
            and not self.source.datasources[0].name == CONSTANT_DATASET
        )

    @property
    def base_name(self) -> str:
        if self.base_name_override:
            return self.base_name_override
        # if this cte selects from a single datasource, select right from it
        if self.is_root_datasource:
            return self.source.datasources[0].safe_location

        # if we have multiple joined CTEs, pick the base
        # as the root
        elif len(self.source.datasources) == 1 and len(self.parent_ctes) == 1:
            return self.parent_ctes[0].name
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        return self.source.name

    @property
    def base_alias(self) -> str:
        if self.base_alias_override:
            return self.base_alias_override
        if self.is_root_datasource:
            return self.source.datasources[0].identifier
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        elif self.parent_ctes:
            return self.parent_ctes[0].name
        return self.name

    def get_alias(self, concept: Concept, source: str | None = None) -> str:
        for cte in self.parent_ctes:
            if concept.address in [x.address for x in cte.output_columns]:
                if source and source != cte.name:
                    continue
                return concept.safe_address
        try:
            source = self.source.get_alias(concept, source=source)
            if not source:
                raise ValueError("No source found")
            return source
        except ValueError as e:
            return f"INVALID_ALIAS: {str(e)}"

    @property
    def render_from_clause(self) -> bool:
        if (
            all([c.derivation == PurposeLineage.CONSTANT for c in self.output_columns])
            and not self.parent_ctes
            and not self.group_to_grain
        ):
            return False
        if (
            len(self.source.datasources) == 1
            and self.source.datasources[0].name == CONSTANT_DATASET
        ):
            return False
        return True

    @property
    def sourced_concepts(self) -> List[Concept]:
        return [c for c in self.output_columns if c.address in self.source_map]


def merge_ctes(ctes: List[CTE]) -> List[CTE]:
    final_ctes_dict: Dict[str, CTE] = {}
    # merge CTEs
    for cte in ctes:
        if cte.name not in final_ctes_dict:
            final_ctes_dict[cte.name] = cte
        else:
            final_ctes_dict[cte.name] = final_ctes_dict[cte.name] + cte

    final_ctes = list(final_ctes_dict.values())
    return final_ctes


class CompiledCTE(BaseModel):
    name: str
    statement: str


class JoinKey(BaseModel):
    concept: Concept

    def __str__(self):
        return str(self.concept)


class Join(BaseModel):
    left_cte: CTE | Datasource
    right_cte: CTE | Datasource
    jointype: JoinType
    joinkeys: List[JoinKey]

    @property
    def left_name(self) -> str:
        if isinstance(self.left_cte, Datasource):
            return self.left_cte.identifier
        return self.left_cte.name

    @property
    def right_name(self) -> str:
        if isinstance(self.right_cte, Datasource):
            return self.right_cte.identifier
        return self.right_cte.name

    @property
    def left_ref(self) -> str:
        if isinstance(self.left_cte, Datasource):
            return f"{self.left_cte.safe_location} as {self.left_cte.identifier}"
        return self.left_cte.name

    @property
    def right_ref(self) -> str:
        if isinstance(self.right_cte, Datasource):
            return f"{self.right_cte.safe_location} as {self.right_cte.identifier}"
        return self.right_cte.name

    @property
    def unique_id(self) -> str:
        return self.left_name + self.right_name + self.jointype.value

    def __str__(self):
        return (
            f"{self.jointype.value} JOIN {self.left_name} and"
            f" {self.right_name} on {','.join([str(k) for k in self.joinkeys])}"
        )


class UndefinedConcept(Concept):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str
    environment: "EnvironmentConceptDict"
    line_no: int | None = None
    datatype: DataType = DataType.UNKNOWN
    purpose: Purpose = Purpose.KEY

    def with_namespace(self, namespace: str) -> "UndefinedConcept":
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage.with_namespace(namespace) if self.lineage else None,
            grain=(
                self.grain.with_namespace(namespace)
                if self.grain
                else Grain(components=[])
            ),
            namespace=namespace,
            keys=self.keys,
            environment=self.environment,
            line_no=self.line_no,
        )

    def with_select_grain(self, grain: Optional["Grain"] = None) -> "UndefinedConcept":
        if not all([isinstance(x, Concept) for x in self.keys or []]):
            raise ValueError(f"Invalid keys {self.keys} for concept {self.address}")
        new_grain = grain or Grain(components=[])
        if self.lineage:
            new_lineage = self.lineage
            if isinstance(self.lineage, SelectGrain):
                new_lineage = self.lineage.with_select_grain(new_grain)
        else:
            new_lineage = None
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=new_lineage,
            grain=new_grain,
            namespace=self.namespace,
            keys=self.keys,
            environment=self.environment,
        )

    def with_grain(self, grain: Optional["Grain"] = None) -> "Concept":
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain or Grain(components=[]),
            namespace=self.namespace,
            keys=self.keys,
            environment=self.environment,
            line_no=self.line_no,
        )

    def with_default_grain(self) -> "Concept":
        if self.purpose == Purpose.KEY:
            # we need to make this abstract
            grain = Grain(components=[self.with_grain(Grain())], nested=True)
        elif self.purpose == Purpose.PROPERTY:
            components: List[Concept] = []
            if self.keys:
                components = [*self.keys]
            if self.lineage:
                for item in self.lineage.arguments:
                    if isinstance(item, Concept):
                        if item.keys and not all(c in components for c in item.keys):
                            components += item.sources
                        else:
                            components += item.sources
            grain = Grain(components=components)
        elif self.purpose == Purpose.METRIC:
            grain = Grain()
        else:
            grain = self.grain  # type: ignore
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain,
            keys=self.keys,
            namespace=self.namespace,
            environment=self.environment,
            line_no=self.line_no,
        )


class EnvironmentConceptDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)
        self.undefined: dict[str, UndefinedConcept] = {}
        self.fail_on_missing: bool = False
        self.populate_default_concepts()

    def populate_default_concepts(self):
        from trilogy.core.internal import DEFAULT_CONCEPTS

        for concept in DEFAULT_CONCEPTS.values():
            self[concept.address] = concept

    def values(self) -> ValuesView[Concept]:  # type: ignore
        return super().values()

    def __getitem__(
        self, key, line_no: int | None = None
    ) -> Concept | UndefinedConcept:
        try:
            return super(EnvironmentConceptDict, self).__getitem__(key)

        except KeyError:
            if "." in key and key.split(".")[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".")[1], line_no)
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key, line_no)
            if not self.fail_on_missing:
                undefined = UndefinedConcept(
                    name=key,
                    line_no=line_no,
                    environment=self,
                    datatype=DataType.UNKNOWN,
                    purpose=Purpose.KEY,
                )
                self.undefined[key] = undefined
                return undefined

            matches = self._find_similar_concepts(key)
            message = f"Undefined concept: {key}."
            if matches:
                message += f" Suggestions: {matches}"

            if line_no:
                raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
            raise UndefinedConceptException(message, matches)

    def _find_similar_concepts(self, concept_name):
        matches = difflib.get_close_matches(concept_name, self.keys())
        return matches

    def items(self) -> ItemsView[str, Concept | UndefinedConcept]:  # type: ignore
        return super().items()


class ImportStatement(BaseModel):
    alias: str
    path: Path
    environment: Union["Environment", None] = None
    # TODO: this might result in a lot of duplication
    # environment:"Environment"


class EnvironmentOptions(BaseModel):
    allow_duplicate_declaration: bool = True


def validate_concepts(v) -> EnvironmentConceptDict:
    if isinstance(v, EnvironmentConceptDict):
        return v
    elif isinstance(v, dict):
        return EnvironmentConceptDict(
            **{x: Concept.model_validate(y) for x, y in v.items()}
        )
    raise ValueError


class Environment(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, strict=False)

    concepts: Annotated[EnvironmentConceptDict, PlainValidator(validate_concepts)] = (
        Field(default_factory=EnvironmentConceptDict)
    )
    datasources: Dict[str, Datasource] = Field(default_factory=dict)
    functions: Dict[str, Function] = Field(default_factory=dict)
    data_types: Dict[str, DataType] = Field(default_factory=dict)
    imports: Dict[str, ImportStatement] = Field(default_factory=dict)
    namespace: str = DEFAULT_NAMESPACE
    working_path: str | Path = Field(default_factory=lambda: os.getcwd())
    environment_config: EnvironmentOptions = Field(default_factory=EnvironmentOptions)
    version: str = Field(default_factory=get_version)
    cte_name_map: Dict[str, str] = Field(default_factory=dict)

    materialized_concepts: List[Concept] = Field(default_factory=list)
    merged_concepts: Dict[str, Concept] = Field(default_factory=dict)
    _parse_count: int = 0

    @classmethod
    def from_file(cls, path: str | Path) -> "Environment":
        with open(path, "r") as f:
            read = f.read()
        return Environment(working_path=Path(path).parent).parse(read)[0]

    @classmethod
    def from_cache(cls, path) -> Optional["Environment"]:
        with open(path, "r") as f:
            read = f.read()
        base = cls.model_validate_json(read)
        version = get_version()
        if base.version != version:
            return None
        return base

    def to_cache(self, path: Optional[str | Path] = None) -> Path:
        if not path:
            ppath = Path(self.working_path) / ENV_CACHE_NAME
        else:
            ppath = Path(path)
        with open(ppath, "w") as f:
            f.write(self.model_dump_json())
        return ppath

    def gen_concept_list_caches(self) -> None:
        concrete_addresses = set()
        for datasource in self.datasources.values():
            for concept in datasource.output_concepts:
                concrete_addresses.add(concept.address)
        current_mat = [x.address for x in self.materialized_concepts]
        self.materialized_concepts = [
            c for c in self.concepts.values() if c.address in concrete_addresses
        ]
        new = [
            x.address
            for x in self.materialized_concepts
            if x.address not in current_mat
        ]
        if new:
            logger.info(f"Environment added new materialized concepts {new}")
        for concept in self.concepts.values():
            if concept.derivation == PurposeLineage.MERGE:
                ms = concept.lineage
                assert isinstance(ms, MergeStatement)
                for parent in ms.concepts:
                    self.merged_concepts[parent.address] = concept

    def validate_concept(self, lookup: str, meta: Meta | None = None):
        existing: Concept = self.concepts.get(lookup)  # type: ignore
        if not existing:
            return
        elif existing and self.environment_config.allow_duplicate_declaration:
            return
        elif existing.metadata:
            # if the existing concept is auto derived, we can overwrite it
            if existing.metadata.concept_source == ConceptSource.AUTO_DERIVED:
                return
        elif meta and existing.metadata:
            raise ValueError(
                f"Assignment to concept '{lookup}' on line {meta.line} is a duplicate"
                f" declaration; '{lookup}' was originally defined on line"
                f" {existing.metadata.line_number}"
            )
        elif existing.metadata:
            raise ValueError(
                f"Assignment to concept '{lookup}'  is a duplicate declaration;"
                f" '{lookup}' was originally defined on line"
                f" {existing.metadata.line_number}"
            )
        raise ValueError(
            f"Assignment to concept '{lookup}'  is a duplicate declaration;"
        )

    def add_import(self, alias: str, environment: Environment):
        self.imports[alias] = ImportStatement(
            alias=alias, path=Path(environment.working_path)
        )
        for key, concept in environment.concepts.items():
            self.concepts[f"{alias}.{key}"] = concept.with_namespace(alias)
        for key, datasource in environment.datasources.items():
            self.datasources[f"{alias}.{key}"] = datasource.with_namespace(alias)
        self.gen_concept_list_caches()
        return self

    def add_file_import(self, path: str, alias: str, env: Environment | None = None):
        from trilogy.parsing.parse_engine import ParseToObjects, PARSER

        apath = path.split(".")
        apath[-1] = apath[-1] + ".preql"

        target: Path = Path(self.working_path, *apath)
        if env:
            self.imports[alias] = ImportStatement(
                alias=alias, path=target, environment=env
            )

        elif alias in self.imports:
            current = self.imports[alias]
            env = self.imports[alias].environment
            if current.path != target:
                raise ImportError(
                    f"Attempted to import {target} with alias {alias} but {alias} is already imported from {current.path}"
                )
        else:
            try:
                with open(target, "r", encoding="utf-8") as f:
                    text = f.read()
                nparser = ParseToObjects(
                    visit_tokens=True,
                    text=text,
                    environment=Environment(
                        working_path=target.parent,
                    ),
                    parse_address=str(target),
                )
                nparser.transform(PARSER.parse(text))
            except Exception as e:
                raise ImportError(
                    f"Unable to import file {target.parent}, parsing error: {e}"
                )
            env = nparser.environment
        if env:
            for _, concept in env.concepts.items():
                self.add_concept(concept.with_namespace(alias))

            for _, datasource in env.datasources.items():
                self.add_datasource(datasource.with_namespace(alias))
        imps = ImportStatement(alias=alias, path=target, environment=env)
        self.imports[alias] = imps
        return imps

    def parse(
        self, input: str, namespace: str | None = None, persist: bool = False
    ) -> Tuple[Environment, list]:
        from trilogy import parse
        from trilogy.core.query_processor import process_persist

        if namespace:
            new = Environment()
            _, queries = new.parse(input)
            self.add_import(namespace, new)
            return self, queries
        _, queries = parse(input, self)
        generatable = [
            x
            for x in queries
            if isinstance(
                x,
                (
                    SelectStatement,
                    PersistStatement,
                    MultiSelectStatement,
                    ShowStatement,
                ),
            )
        ]
        while generatable:
            t = generatable.pop(0)
            if isinstance(t, PersistStatement) and persist:
                processed = process_persist(self, t)
                self.add_datasource(processed.datasource)
        return self, queries

    def add_concept(
        self,
        concept: Concept,
        meta: Meta | None = None,
        force: bool = False,
        add_derived: bool = True,
    ):
        if not force:
            self.validate_concept(concept.address, meta=meta)
        if concept.namespace == DEFAULT_NAMESPACE:
            self.concepts[concept.name] = concept
        else:
            self.concepts[concept.address] = concept
        if add_derived:
            from trilogy.core.environment_helpers import generate_related_concepts

            generate_related_concepts(concept, self)
        self.gen_concept_list_caches()
        return concept

    def add_datasource(
        self,
        datasource: Datasource,
        meta: Meta | None = None,
    ):
        if not datasource.namespace or datasource.namespace == DEFAULT_NAMESPACE:
            self.datasources[datasource.name] = datasource
            self.gen_concept_list_caches()
            return datasource
        self.datasources[datasource.namespace + "." + datasource.identifier] = (
            datasource
        )
        self.gen_concept_list_caches()
        return datasource

    def delete_datasource(
        self,
        address: str,
        meta: Meta | None = None,
    ) -> bool:
        if address in self.datasources:
            del self.datasources[address]
            self.gen_concept_list_caches()
            return True
        return False


class LazyEnvironment(Environment):
    """Variant of environment to defer parsing of a path"""

    load_path: Path
    loaded: bool = False

    def __getattribute__(self, name):
        if name in (
            "load_path",
            "loaded",
            "working_path",
            "model_config",
            "model_fields",
        ) or name.startswith("_"):
            return super().__getattribute__(name)
        if not self.loaded:
            print(f"lazily evaluating load path {self.load_path} to access {name}")
            from trilogy import parse

            env = Environment(working_path=str(self.working_path))
            with open(self.load_path, "r") as f:
                parse(f.read(), env)
            self.loaded = True
            self.datasources = env.datasources
            self.concepts = env.concepts
            self.imports = env.imports
        return super().__getattribute__(name)


class Comparison(ConceptArgs, Namespaced, SelectGrain, BaseModel):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        Function,
        Concept,
        "Conditional",
        DataType,
        "Comparison",
        "Parenthetical",
        MagicConstants,
        WindowItem,
        AggregateWrapper,
    ]
    right: Union[
        int,
        str,
        float,
        list,
        bool,
        Concept,
        Function,
        "Conditional",
        DataType,
        "Comparison",
        "Parenthetical",
        MagicConstants,
        WindowItem,
        AggregateWrapper,
    ]
    operator: ComparisonOperator

    def __post_init__(self):
        if arg_to_datatype(self.left) != arg_to_datatype(self.right):
            raise SyntaxError(
                f"Cannot compare {self.left} and {self.right} of different types"
            )
        if self.operator == ComparisonOperator.BETWEEN:
            if (
                not isinstance(self.right, ComparisonOperator)
                and self.right.operator == BooleanOperator.AND
            ):
                raise SyntaxError(
                    f"Between operator must have two operands with and, not {self.right}"
                )

    def __add__(self, other):
        if not isinstance(other, (Comparison, Conditional, Parenthetical)):
            raise ValueError("Cannot add Comparison to non-Comparison")
        if other == self:
            return self
        return Conditional(left=self, right=other, operator=BooleanOperator.AND)

    def __repr__(self):
        return f"{str(self.left)} {self.operator.value} {str(self.right)}"

    def __str__(self):
        return self.__repr__()

    def with_namespace(self, namespace: str):
        return self.__class__(
            left=(
                self.left.with_namespace(namespace)
                if isinstance(self.left, Namespaced)
                else self.left
            ),
            right=(
                self.right.with_namespace(namespace)
                if isinstance(self.right, Namespaced)
                else self.right
            ),
            operator=self.operator,
        )

    def with_select_grain(self, grain: Grain):
        return self.__class__(
            left=(
                self.left.with_select_grain(grain)
                if isinstance(self.left, SelectGrain)
                else self.left
            ),
            # the right side does NOT need to inherit select grain
            right=self.right,
            operator=self.operator,
        )

    @property
    def input(self) -> List[Concept]:
        output: List[Concept] = []
        if isinstance(self.left, (Concept,)):
            output += [self.left]
        if isinstance(
            self.left, (Comparison, SubselectComparison, Conditional, Parenthetical)
        ):
            output += self.left.input
        if isinstance(self.left, FilterItem):
            output += self.left.concept_arguments
        if isinstance(self.left, Function):
            output += self.left.concept_arguments

        if isinstance(self.right, (Concept,)):
            output += [self.right]
        if isinstance(
            self.right, (Comparison, SubselectComparison, Conditional, Parenthetical)
        ):
            output += self.right.input
        if isinstance(self.right, FilterItem):
            output += self.right.concept_arguments
        if isinstance(self.right, Function):
            output += self.right.concept_arguments
        return output

    @property
    def concept_arguments(self) -> List[Concept]:
        """Return concepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[Concept]:
        output = []
        if isinstance(self.left, ConceptArgs):
            output += self.left.row_arguments
        else:
            output += get_concept_arguments(self.left)
        if isinstance(self.right, ConceptArgs):
            output += self.right.row_arguments
        else:
            output += get_concept_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> List[Tuple[Concept, ...]]:
        """Return concepts directly referenced in where clause"""
        output: List[Tuple[Concept, ...]] = []
        if isinstance(self.left, ConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, ConceptArgs):
            output += self.right.existence_arguments
        return output


class SubselectComparison(Comparison):

    @property
    def row_arguments(self) -> List[Concept]:
        return get_concept_arguments(self.left)

    @property
    def existence_arguments(self) -> list[tuple["Concept", ...]]:
        return [tuple(get_concept_arguments(self.right))]

    def with_select_grain(self, grain: Grain):
        # there's no need to pass the select grain through to a subselect comparison
        return self.__class__(
            left=(
                self.left.with_select_grain(grain)
                if isinstance(self.left, SelectGrain)
                else self.left
            ),
            right=self.right,
            operator=self.operator,
        )


class CaseWhen(Namespaced, SelectGrain, BaseModel):
    comparison: Conditional | SubselectComparison | Comparison
    expr: "Expr"

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.comparison) + get_concept_arguments(self.expr)

    def with_namespace(self, namespace: str) -> CaseWhen:
        return CaseWhen(
            comparison=self.comparison.with_namespace(namespace),
            expr=(
                self.expr.with_namespace(namespace)
                if isinstance(
                    self.expr,
                    Namespaced,
                )
                else self.expr
            ),
        )

    def with_select_grain(self, grain: Grain) -> CaseWhen:
        return CaseWhen(
            comparison=self.comparison.with_select_grain(grain),
            expr=(
                (self.expr.with_select_grain(grain))
                if isinstance(self.expr, SelectGrain)
                else self.expr
            ),
        )


class CaseElse(Namespaced, SelectGrain, BaseModel):
    expr: "Expr"
    # this ensures that it's easily differentiable from CaseWhen
    discriminant: ComparisonOperator = ComparisonOperator.ELSE

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)

    def with_select_grain(self, grain: Grain) -> CaseElse:
        return CaseElse(
            discriminant=self.discriminant,
            expr=(
                self.expr.with_select_grain(grain)
                if isinstance(
                    self.expr,
                    SelectGrain,
                )
                else self.expr
            ),
        )

    def with_namespace(self, namespace: str) -> CaseElse:
        return CaseElse(
            discriminant=self.discriminant,
            expr=(
                self.expr.with_namespace(namespace)
                if isinstance(
                    self.expr,
                    Namespaced,
                )
                else self.expr
            ),
        )


class Conditional(ConceptArgs, Namespaced, SelectGrain, BaseModel):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        Concept,
        Comparison,
        "Conditional",
        "Parenthetical",
        Function,
        FilterItem,
    ]
    right: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        Concept,
        Comparison,
        "Conditional",
        "Parenthetical",
        Function,
        FilterItem,
    ]
    operator: BooleanOperator

    def __add__(self, other) -> "Conditional":
        if other is None:
            return self
        elif str(other) == str(self):
            return self
        elif isinstance(other, (Comparison, Conditional, Parenthetical)):
            return Conditional(left=self, right=other, operator=BooleanOperator.AND)
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{str(self.left)} {self.operator.value} {str(self.right)}"

    def with_namespace(self, namespace: str):
        return Conditional(
            left=(
                self.left.with_namespace(namespace)
                if isinstance(self.left, Namespaced)
                else self.left
            ),
            right=(
                self.right.with_namespace(namespace)
                if isinstance(self.right, Namespaced)
                else self.right
            ),
            operator=self.operator,
        )

    def with_select_grain(self, grain: Grain):
        return Conditional(
            left=(
                self.left.with_select_grain(grain)
                if isinstance(self.left, SelectGrain)
                else self.left
            ),
            right=(
                self.right.with_select_grain(grain)
                if isinstance(self.right, SelectGrain)
                else self.right
            ),
            operator=self.operator,
        )

    @property
    def input(self) -> List[Concept]:
        """Return concepts directly referenced in where clause"""
        output = []

        for x in (self.left, self.right):
            if isinstance(x, Concept):
                output += x.input
            elif isinstance(x, (Comparison, Conditional)):
                output += x.input
            elif isinstance(x, (Function, Parenthetical, FilterItem)):
                output += x.concept_arguments
        return output

    @property
    def concept_arguments(self) -> List[Concept]:
        """Return concepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[Concept]:
        output = []
        if isinstance(self.left, ConceptArgs):
            output += self.left.row_arguments
        else:
            output += get_concept_arguments(self.left)
        if isinstance(self.right, ConceptArgs):
            output += self.right.row_arguments
        else:
            output += get_concept_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> list[tuple["Concept", ...]]:
        output = []
        if isinstance(self.left, ConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, ConceptArgs):
            output += self.right.existence_arguments
        return output

    def decompose(self):
        chunks = []
        if self.operator == BooleanOperator.AND:
            for val in [self.left, self.right]:
                if isinstance(val, Conditional):
                    chunks.extend(val.decompose())
                else:
                    chunks.append(val)
        else:
            chunks.append(self)
        return chunks


class AggregateWrapper(Namespaced, SelectGrain, BaseModel):
    function: Function
    by: List[Concept] = Field(default_factory=list)

    def __str__(self):
        grain_str = [str(c) for c in self.by] if self.by else "abstract"
        return f"{str(self.function)}<{grain_str}>"

    @property
    def datatype(self):
        return self.function.datatype

    @property
    def concept_arguments(self) -> List[Concept]:
        return self.function.concept_arguments + self.by

    @property
    def output_datatype(self):
        return self.function.output_datatype

    @property
    def output_purpose(self):
        return self.function.output_purpose

    @property
    def arguments(self):
        return self.function.arguments

    def with_namespace(self, namespace: str) -> "AggregateWrapper":
        return AggregateWrapper(
            function=self.function.with_namespace(namespace),
            by=[c.with_namespace(namespace) for c in self.by] if self.by else [],
        )

    def with_select_grain(self, grain: Grain) -> AggregateWrapper:
        if not self.by:
            by = grain.components_copy
        else:
            by = self.by
        return AggregateWrapper(function=self.function.with_select_grain(grain), by=by)


class WhereClause(ConceptArgs, Namespaced, SelectGrain, BaseModel):
    conditional: Union[SubselectComparison, Comparison, Conditional, "Parenthetical"]

    @property
    def input(self) -> List[Concept]:
        return self.conditional.input

    @property
    def concept_arguments(self) -> List[Concept]:
        return self.conditional.concept_arguments

    @property
    def row_arguments(self) -> List[Concept]:
        return self.conditional.row_arguments

    @property
    def existence_arguments(self) -> list[tuple["Concept", ...]]:
        return self.conditional.existence_arguments

    def with_namespace(self, namespace: str) -> WhereClause:
        return WhereClause(conditional=self.conditional.with_namespace(namespace))

    def with_select_grain(self, grain: Grain) -> WhereClause:
        return WhereClause(conditional=self.conditional.with_select_grain(grain))

    @property
    def grain(self) -> Grain:
        output = []
        for item in self.input:
            if item.purpose == Purpose.KEY:
                output.append(item)
            elif item.purpose == Purpose.PROPERTY:
                output += item.grain.components if item.grain else []
        return Grain(components=list(set(output)))


class MaterializedDataset(BaseModel):
    address: Address


# TODO: combine with CTEs
# CTE contains procesed query?
# or CTE references CTE?


class ProcessedQuery(BaseModel):
    output_columns: List[Concept]
    ctes: List[CTE]
    base: CTE
    joins: List[Join]
    grain: Grain
    hidden_columns: List[Concept] = Field(default_factory=list)
    limit: Optional[int] = None
    where_clause: Optional[WhereClause] = None
    order_by: Optional[OrderBy] = None


class ProcessedQueryMixin(BaseModel):
    output_to: MaterializedDataset
    datasource: Datasource
    # base:Dataset


class ProcessedQueryPersist(ProcessedQuery, ProcessedQueryMixin):
    pass


class ProcessedShowStatement(BaseModel):
    output_columns: List[Concept]
    output_values: List[Union[Concept, Datasource, ProcessedQuery]]


class Limit(BaseModel):
    count: int


class ConceptDeclarationStatement(BaseModel):
    concept: Concept


class ConceptDerivation(BaseModel):
    concept: Concept


class RowsetDerivationStatement(Namespaced, BaseModel):
    name: str
    select: SelectStatement | MultiSelectStatement
    namespace: str

    def __repr__(self):
        return f"RowsetDerivation<{str(self.select)}>"

    @property
    def derived_concepts(self) -> List[Concept]:
        output: list[Concept] = []
        orig: dict[str, Concept] = {}
        for orig_concept in self.select.output_components:
            new_concept = Concept(
                name=orig_concept.name,
                datatype=orig_concept.datatype,
                purpose=orig_concept.purpose,
                lineage=RowsetItem(
                    content=orig_concept, where=self.select.where_clause, rowset=self
                ),
                grain=orig_concept.grain,
                metadata=orig_concept.metadata,
                namespace=(
                    f"{self.name}.{orig_concept.namespace}"
                    if orig_concept.namespace != self.namespace
                    else self.name
                ),
                keys=orig_concept.keys,
            )
            orig[orig_concept.address] = new_concept
            output.append(new_concept)
        default_grain = Grain(components=[*output])
        # remap everything to the properties of the rowset
        for x in output:
            if x.keys:
                if all([k.address in orig for k in x.keys]):
                    x.keys = tuple(
                        [orig[k.address] if k.address in orig else k for k in x.keys]
                    )
                else:
                    # TODO: fix this up
                    x.keys = tuple()
        for x in output:
            if all([c.address in orig for c in x.grain.components_copy]):
                x.grain = Grain(
                    components=[orig[c.address] for c in x.grain.components_copy]
                )
            else:
                x.grain = default_grain
        return output

    @property
    def arguments(self) -> List[Concept]:
        return self.select.output_components

    def with_namespace(self, namespace: str) -> "RowsetDerivationStatement":
        return RowsetDerivationStatement(
            name=self.name,
            select=self.select.with_namespace(namespace),
            namespace=namespace,
        )


class RowsetItem(Namespaced, BaseModel):
    content: Concept
    rowset: RowsetDerivationStatement
    where: Optional["WhereClause"] = None

    def __repr__(self):
        return (
            f"<Rowset<{self.rowset.name}>: {str(self.content)} where {str(self.where)}>"
        )

    def with_namespace(self, namespace: str) -> "RowsetItem":
        return RowsetItem(
            content=self.content.with_namespace(namespace),
            where=self.where.with_namespace(namespace) if self.where else None,
            rowset=self.rowset.with_namespace(namespace),
        )

    @property
    def arguments(self) -> List[Concept]:
        output = [self.content]
        if self.where:
            output += self.where.input
        return output

    @property
    def output(self) -> Concept:
        if isinstance(self.content, ConceptTransform):
            return self.content.output
        return self.content

    @output.setter
    def output(self, value):
        if isinstance(self.content, ConceptTransform):
            self.content.output = value
        else:
            self.content = value

    @property
    def input(self) -> List[Concept]:
        base = self.content.input
        if self.where:
            base += self.where.input
        return base

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def output_purpose(self):
        return self.content.purpose

    @property
    def concept_arguments(self):
        if self.where:
            return [self.content] + self.where.concept_arguments
        return [self.content]


class Parenthetical(ConceptArgs, Namespaced, SelectGrain, BaseModel):
    content: "Expr"

    def __str__(self):
        return self.__repr__()

    def __add__(self, other) -> Union["Parenthetical", "Conditional"]:
        if other is None:
            return self
        elif isinstance(other, (Comparison, Conditional, Parenthetical)):
            return Conditional(left=self, right=other, operator=BooleanOperator.AND)
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __repr__(self):
        return f"({str(self.content)})"

    def with_namespace(self, namespace: str):
        return Parenthetical(
            content=(
                self.content.with_namespace(namespace)
                if isinstance(self.content, Namespaced)
                else self.content
            )
        )

    def with_select_grain(self, grain: Grain):
        return Parenthetical(
            content=(
                self.content.with_select_grain(grain)
                if isinstance(self.content, SelectGrain)
                else self.content
            )
        )

    @property
    def concept_arguments(self) -> List[Concept]:
        base: List[Concept] = []
        x = self.content
        if hasattr(x, "concept_arguments"):
            base += x.concept_arguments
        elif isinstance(x, Concept):
            base.append(x)
        return base

    @property
    def row_arguments(self) -> List[Concept]:
        if isinstance(self.content, ConceptArgs):
            return self.content.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> list[tuple["Concept", ...]]:
        if isinstance(self.content, ConceptArgs):
            return self.content.existence_arguments
        return []

    @property
    def input(self):
        base = []
        x = self.content
        if hasattr(x, "input"):
            base += x.input
        return base


class PersistStatement(BaseModel):
    datasource: Datasource
    select: SelectStatement

    @property
    def identifier(self):
        return self.datasource.identifier

    @property
    def address(self):
        return self.datasource.address


class ShowStatement(BaseModel):
    content: SelectStatement | PersistStatement | ShowCategory


Expr = (
    bool
    | int
    | str
    | float
    | list
    | WindowItem
    | FilterItem
    | Concept
    | Comparison
    | Conditional
    | Parenthetical
    | Function
    | AggregateWrapper
)


Concept.model_rebuild()
Grain.model_rebuild()
WindowItem.model_rebuild()
WindowItemOrder.model_rebuild()
FilterItem.model_rebuild()
Comparison.model_rebuild()
Conditional.model_rebuild()
Parenthetical.model_rebuild()
WhereClause.model_rebuild()
ImportStatement.model_rebuild()
CaseWhen.model_rebuild()
CaseElse.model_rebuild()
SelectStatement.model_rebuild()
CTE.model_rebuild()
BaseJoin.model_rebuild()
QueryDatasource.model_rebuild()
ProcessedQuery.model_rebuild()
ProcessedQueryPersist.model_rebuild()
InstantiatedUnnestJoin.model_rebuild()
UndefinedConcept.model_rebuild()
Function.model_rebuild()
Grain.model_rebuild()


def list_to_wrapper(args):
    types = [arg_to_datatype(arg) for arg in args]
    assert len(set(types)) == 1
    return ListWrapper(args, type=types[0])


def arg_to_datatype(arg) -> DataType | ListType | StructType | MapType:
    if isinstance(arg, Function):
        return arg.output_datatype
    elif isinstance(arg, Concept):
        return arg.datatype
    elif isinstance(arg, bool):
        return DataType.BOOL
    elif isinstance(arg, int):
        return DataType.INTEGER
    elif isinstance(arg, str):
        return DataType.STRING
    elif isinstance(arg, float):
        return DataType.FLOAT
    elif isinstance(arg, ListWrapper):
        return ListType(type=arg.type)
    elif isinstance(arg, AggregateWrapper):
        return arg.function.output_datatype
    elif isinstance(arg, Parenthetical):
        return arg_to_datatype(arg.content)
    elif isinstance(arg, WindowItem):
        if arg.type in (WindowType.RANK, WindowType.ROW_NUMBER):
            return DataType.INTEGER
        return arg_to_datatype(arg.content)
    elif isinstance(arg, list):
        wrapper = list_to_wrapper(arg)
        return ListType(type=wrapper.type)
    else:
        raise ValueError(f"Cannot parse arg datatype for arg of raw type {type(arg)}")
