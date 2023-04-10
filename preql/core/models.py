import difflib
import os
from copy import deepcopy
from dataclasses import dataclass, field
from typing import (
    Dict,
    MutableMapping,
    TypeVar,
    List,
    Optional,
    Union,
    Set,
    Any,
    Sequence,
)

from pydantic import BaseModel, validator, Field

from preql.constants import logger, DEFAULT_NAMESPACE
from preql.core.enums import (
    DataType,
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
)
from preql.core.exceptions import UndefinedConceptException
from preql.utility import unique

LOGGER_PREFIX = "[MODELS]"

KT = TypeVar("KT")
VT = TypeVar("VT")


class Metadata(BaseModel):
    """Metadata container object.
    TODO: support arbitrary tags"""

    description: Optional[str]
    line_number: Optional[int]


class Concept(BaseModel):
    name: str
    datatype: DataType
    purpose: Purpose
    metadata: Optional[Metadata] = Field(
        default_factory=lambda: Metadata(description=None, line_number=None)
    )
    lineage: Optional[Union["Function", "WindowItem", "FilterItem"]] = None
    namespace: Optional[str] = ""
    keys: Optional[List["Concept"]] = None
    grain: Optional["Grain"] = Field(default=None)

    def __hash__(self):
        return hash(str(self))

    @validator("lineage")
    def lineage_validator(cls, v):
        if v and not isinstance(v, (Function, WindowItem, FilterItem)):
            raise ValueError(v)
        return v

    @validator("metadata")
    def metadata_validation(cls, v):
        v = v or Metadata()
        return v

    @validator("namespace", pre=True, always=True)
    def namespace_enforcement(cls, v):
        if not v:
            return DEFAULT_NAMESPACE
        return v

    def with_namespace(self, namespace: str) -> "Concept":
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage.with_namespace(namespace) if self.lineage else None,
            grain=self.grain.with_namespace(namespace) if self.grain else None,
            namespace=namespace,
            keys=self.keys,
        )

    @validator("grain", pre=True, always=True)
    def parse_grain(cls, v, values):
        # this is silly - rethink how we do grains
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
        elif not v:
            v = Grain(components=[])
        elif isinstance(v, Concept):
            v = Grain(components=[v])
        return v

    def __eq__(self, other: object):
        if not isinstance(other, Concept):
            return False
        return (
            self.name == other.name
            and self.datatype == other.datatype
            and self.purpose == other.purpose
            and self.namespace == other.namespace
            and self.grain == other.grain
            and self.keys == other.keys
        )

    def __str__(self):
        grain = ",".join([str(c.address) for c in self.grain.components])
        return f"{self.namespace}.{self.name}<{grain}>"

    @property
    def address(self) -> str:
        return f"{self.namespace}.{self.name}"

    @property
    def output(self) -> "Concept":
        return self

    @property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}_{self.name}"

    @property
    def grain_components(self) -> List["Concept"]:
        return self.grain.components_copy if self.grain else []

    def with_grain(self, grain: Optional["Grain"] = None) -> "Concept":
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain,
            namespace=self.namespace,
            keys=self.keys,
        )

    def with_default_grain(self) -> "Concept":
        if self.purpose == Purpose.KEY:
            # we need to make this abstract
            grain = Grain(components=[deepcopy(self).with_grain(Grain())], nested=True)
        elif self.purpose == Purpose.PROPERTY:
            components = []
            if self.keys:
                components = self.keys
            if self.lineage:
                for item in self.lineage.arguments:
                    if isinstance(item, Concept):
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
        )

    @property
    def sources(self) -> List["Concept"]:
        if self.lineage:
            output = []
            for item in self.lineage.arguments:
                if isinstance(item, Concept):
                    output.append(item)
                    output += item.sources
            return output
        return []

    @property
    def input(self):
        return [self] + self.sources

    @property
    def derivation(self) -> PurposeLineage:
        if self.lineage and isinstance(self.lineage, WindowItem):
            return PurposeLineage.WINDOW
        if self.lineage and isinstance(self.lineage, FilterItem):
            return PurposeLineage.FILTER
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return PurposeLineage.AGGREGATE
        return PurposeLineage.BASIC


@dataclass(eq=True)
class ColumnAssignment:
    alias: str
    concept: Concept
    modifiers: List[Modifier] = field(default_factory=list)

    def is_complete(self):
        return Modifier.PARTIAL not in self.modifiers

    def with_namespace(self, namespace: str) -> "ColumnAssignment":
        # this breaks assignments
        # TODO: figure out why
        return self
        # return ColumnAssignment(
        #     alias=self.alias,
        #     concept=self.concept.with_namespace(namespace),
        #     modifiers=self.modifiers,
        # )


@dataclass(eq=True, frozen=True)
class Statement:
    pass


class Function(BaseModel):
    operator: FunctionType
    arg_count: int = Field(default=1)
    output_datatype: DataType
    output_purpose: Purpose
    valid_inputs: Optional[Union[Set[DataType], List[Set[DataType]]]] = None
    arguments: List[Union[Concept, int, float, str, DataType, "Function"]]

    @property
    def datatype(self):
        return self.output_datatype

    @validator("arguments", pre=True, always=True)
    def parse_arguments(cls, v, **kwargs):
        from preql.parsing.exceptions import ParseError

        arg_count = len(v)
        target_arg_count = kwargs["values"]["arg_count"]
        operator_name = kwargs["values"]["operator"].name
        valid_inputs = kwargs["values"]["valid_inputs"]
        if not arg_count <= target_arg_count:
            raise ParseError(
                f"Incorrect argument count to {operator_name} function, expects {target_arg_count}, got {arg_count}"
            )
        # for arg in v:
        #     if isinstance(arg, Function):
        #         raise ParseError(
        #             f"Anonymous function calls not allowed; map function to a concept, then pass in. {arg.operator.name} being passed into {operator_name}"
        #         )
        # if all arguments need to be the same type
        # turn this into an array for validation
        if isinstance(valid_inputs, set):
            valid_inputs = [valid_inputs for _ in v]
        elif not valid_inputs:
            return v
        for idx, arg in enumerate(v):

            if isinstance(arg, Concept) and arg.datatype not in valid_inputs[idx]:
                raise TypeError(
                    f"Invalid input datatype {arg.datatype} passed into {operator_name} from concept {arg.name}"
                )
            if (
                isinstance(arg, Function)
                and arg.output_datatype not in valid_inputs[idx]
            ):
                raise TypeError(
                    f"Invalid input datatype {arg.output_datatype} passed into {operator_name} from function {arg.operator.name}"
                )
            # check constants
            for ptype, dtype in [
                [str, DataType.STRING],
                [int, DataType.INTEGER],
                [float, DataType.FLOAT],
            ]:
                if isinstance(arg, ptype) and dtype in valid_inputs[idx]:
                    # attempt to exit early to avoid checking all types
                    break
                elif isinstance(arg, ptype):
                    raise TypeError(
                        f"Invalid {dtype} constant passed into {operator_name} {arg}"
                    )
        return v

    def with_namespace(self, namespace: str) -> "Function":
        return Function(
            operator=self.operator,
            arguments=[
                c.with_namespace(namespace) if isinstance(c, Concept) else c
                for c in self.arguments
            ],
            output_datatype=self.output_datatype,
            output_purpose=self.output_purpose,
            valid_inputs=self.valid_inputs,
        )

    @property
    def concept_arguments(self) -> List[Concept]:
        base = [c for c in self.arguments if isinstance(c, Concept)]
        for arg in self.arguments:
            if isinstance(arg, Function):
                base += arg.concept_arguments
        return base

    @property
    def output_grain(self):
        # aggregates have an abstract grain
        base_grain = Grain(components=[])
        if self.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return base_grain
        # scalars have implicit grain of all arguments
        # for input in self.concept_arguments:
        #
        #     base_grain += input.grain
        return base_grain


@dataclass(eq=True)
class ConceptTransform:
    function: Function
    output: Concept
    modifiers: List[Modifier] = field(default_factory=list)

    @property
    def input(self) -> List[Concept]:
        return [v for v in self.function.arguments if isinstance(v, Concept)]


@dataclass
class Window:
    count: int
    window_order: WindowOrder

    def __str__(self):
        return f"Window<{self.window_order}>"


class WindowItemOver(BaseModel):
    contents: List[Concept]


class WindowItemOrder(BaseModel):
    contents: List["OrderItem"]


class WindowItem(BaseModel):
    type: WindowType
    content: Concept
    order_by: List["OrderItem"]
    over: List["Concept"] = Field(default_factory=list)

    def with_namespace(self, namespace: str) -> "WindowItem":
        return WindowItem(
            type=self.type,
            content=self.content.with_namespace(namespace),
            over=[x.with_namespace(namespace) for x in self.over],
            order_by=[x.with_namespace(namespace) for x in self.order_by],
        )

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
        return self.content.purpose


class FilterItem(BaseModel):
    content: Concept
    where: "WhereClause"

    def with_namespace(self, namespace: str) -> "FilterItem":
        return FilterItem(
            content=self.content.with_namespace(namespace),
            where=self.where.with_namespace(namespace),
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


@dataclass(eq=True)
class SelectItem:
    content: Union[Concept, ConceptTransform]
    modifiers: List[Modifier] = field(default_factory=list)

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


class OrderItem(BaseModel):
    expr: Concept
    order: Ordering

    def with_namespace(self, namespace: str) -> "OrderItem":
        return OrderItem(expr=self.expr.with_namespace(namespace), order=self.order)

    @property
    def input(self):
        return self.expr.input

    @property
    def output(self):
        return self.expr.output


@dataclass(eq=True, frozen=True)
class OrderBy:
    items: List[OrderItem]


@dataclass(eq=True)
class Select:
    selection: Union[List[SelectItem], List[Union[Concept, ConceptTransform]]]
    where_clause: Optional["WhereClause"] = None
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None

    def __post_init__(self):
        final = []
        for item in self.selection:
            if isinstance(item, (Concept, ConceptTransform)):
                final.append(SelectItem(item))
            else:
                final.append(item)
        self.selection = final

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
            elif Modifier.HIDDEN not in item.modifiers:
                output.append(item.output)
        return output

    @property
    def all_components(self) -> List[Concept]:
        return (
            self.input_components + self.output_components + self.grain.components_copy
        )

    @property
    def grain(self) -> "Grain":
        output = []
        for item in self.output_components:
            if item.purpose == Purpose.KEY:
                output.append(item)
        if self.where_clause:
            for item in self.where_clause.input:
                if item.purpose == Purpose.KEY:
                    output.append(item)
                # elif item.purpose == Purpose.PROPERTY and item.grain:
                #     output += item.grain.components
            # TODO: handle other grain cases
            # new if block be design
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
        return Grain(components=unique(output, "address"))


@dataclass(eq=True, frozen=True)
class Address:
    location: str


@dataclass(eq=True, frozen=True)
class Query:
    text: str


class Grain(BaseModel):
    components: List[Concept] = Field(default_factory=list)
    nested: bool = False

    def __init__(self, **kwargs):
        if not kwargs.get("nested", False):
            kwargs["components"] = [
                c.with_default_grain() for c in kwargs.get("components", [])
            ]
        kwargs["components"] = unique(kwargs["components"], "address")
        super().__init__(**kwargs)

    @property
    def components_copy(self) -> List[Concept]:
        return deepcopy(self.components)

    def __str__(self):
        if self.abstract:
            return "Grain<Abstract>"
        return "Grain<" + ",".join([c.address for c in self.components]) + ">"

    def with_namespace(self, namespace: str) -> "Grain":

        return Grain(
            components=[c.with_namespace(namespace) for c in self.components],
            nested=self.nested,
        )

    @property
    def abstract(self):
        return not self.components

    @property
    def set(self):
        return set([c.address for c in self.components_copy])

    def __eq__(self, other: object):
        if not isinstance(other, Grain):
            return False
        return self.set == other.set

    def issubset(self, other: "Grain"):
        return self.set.issubset(other.set)

    def isdisjoint(self, other: "Grain"):
        return self.set.isdisjoint(other.set)

    def intersection(self, other: "Grain") -> "Grain":
        intersection = self.set.intersection(other.set)
        components = [i for i in self.components if i.name in intersection]
        return Grain(components=components)

    def __add__(self, other: "Grain") -> "Grain":
        components = []
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
        return Grain(components=base_components)

    def __radd__(self, other) -> "Grain":
        if other == 0:
            return self
        else:
            return self.__add__(other)


@dataclass
class GrainWindow:
    window: Window
    sort_concepts: List[Concept]

    def __str__(self):
        return (
            "GrainWindow<"
            + ",".join([c.address for c in self.sort_concepts])
            + f":{str(self.window)}>"
        )


@dataclass
class Datasource:
    identifier: str
    columns: List[ColumnAssignment]
    address: Union[Address, str]
    grain: Grain = field(default_factory=lambda: Grain(components=[]))
    namespace: Optional[str] = ""

    def __add__(self, other):
        if not other == self:
            raise ValueError(
                "Attempted to add two datasources that are not identical, this should never happen"
            )
        return self

    def __str__(self):
        return f"{self.namespace}.{self.identifier}@<{self.grain}>"

    def __hash__(self):
        return (self.namespace + self.identifier).__hash__()

    def __post_init__(self):
        # if a user skips defining a grain, use the defined keys
        if not self.grain or not self.grain.components:
            self.grain = Grain(
                components=[
                    deepcopy(v).with_grain(Grain())
                    for v in self.concepts
                    if v.purpose == Purpose.KEY
                ]
            )
        if isinstance(self.address, str):
            self.address = Address(location=self.address)
        if not self.namespace:
            self.namespace = ""

    def with_namespace(self, namespace: str):
        return Datasource(
            identifier=self.identifier,
            namespace=namespace,
            grain=self.grain.with_namespace(namespace),
            address=self.address,
            columns=[c.with_namespace(namespace) for c in self.columns],
        )

    @property
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
    ) -> Optional[str]:
        # 2022-01-22
        # this logic needs to be refined.
        # if concept.lineage:
        # #     return None
        for x in self.columns:
            if x.concept.with_grain(concept.grain) == concept:
                if use_raw_name:
                    return x.alias
                return concept.safe_address
        existing = [str(c.concept.with_grain(self.grain)) for c in self.columns]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {concept} not found on {self.identifier}; have {existing}."
        )

    @property
    def name(self) -> str:
        return self.identifier
        # TODO: namespace all references
        # return f'{self.namespace}_{self.identifier}'

    @property
    def full_name(self) -> str:
        return f"{self.namespace}_{self.identifier}"

    @property
    def safe_location(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address


@dataclass
class BaseJoin:
    left_datasource: Union[Datasource, "QueryDatasource"]
    right_datasource: Union[Datasource, "QueryDatasource"]
    concepts: List[Concept]
    join_type: JoinType
    filter_to_mutual: bool = False

    def __post_init__(self):
        if self.left_datasource.full_name == self.right_datasource.full_name:
            raise SyntaxError(
                f"Cannot join a dataself to itself, joining {self.left_datasource} and {self.right_datasource}"
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
                            f"Invalid join, missing {concept} on {ds.name}, have {[c.address for c in ds.output_concepts]}"
                        )
            if include:
                final_concepts.append(concept)
        if not final_concepts and self.concepts:
            left_keys = [c.address for c in self.left_datasource.output_concepts]
            right_keys = [c.address for c in self.right_datasource.output_concepts]
            raise SyntaxError(
                f"No mutual join keys found between {self.left_datasource.identifier} and {self.right_datasource.identifier}, left_keys {left_keys}, right_keys {right_keys}"
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
        return f'{self.join_type.value} JOIN {self.left_datasource.identifier} and {self.right_datasource.identifier} on {",".join([str(k) for k in self.concepts])}'


@dataclass(eq=True)
class QueryDatasource:
    input_concepts: List[Concept]
    output_concepts: List[Concept]
    source_map: Dict[str, Set[Union[Datasource, "QueryDatasource"]]]
    datasources: Sequence[Union[Datasource, "QueryDatasource"]]
    grain: Grain
    joins: List[BaseJoin]
    limit: Optional[int] = None
    condition: Optional[Union["Conditional", "Comparison"]] = field(default=None)
    filter_concepts: List[Concept] = field(default_factory=list)
    source_type: SourceType = SourceType.SELECT

    def __post_init__(self):
        self.input_concepts = unique(self.input_concepts, "address")
        self.output_concepts = unique(self.output_concepts, "address")

    def __str__(self):
        return f"{self.identifier}@<{self.grain}>"

    def validate(self):
        # validate this was successfully built.
        for concept in self.output_concepts:
            self.get_alias(concept.with_grain(self.grain))

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
        if self.source_type:
            if self.source_type == SourceType.GROUP:
                return True
            return False
        return (
            False if sum([ds.grain for ds in self.datasources]) == self.grain else True
        )

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
        logger.debug(
            f"{LOGGER_PREFIX} merging {self.name} with {[c.address for c in self.output_concepts]} concepts and {other.name} with {[c.address for c in other.output_concepts]} concepts"
        )

        merged_datasources = {}
        for ds in [*self.datasources, *other.datasources]:
            if ds.full_name in merged_datasources:
                merged_datasources[ds.full_name] = merged_datasources[ds.full_name] + ds
            else:
                merged_datasources[ds.full_name] = ds
        return QueryDatasource(
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
            condition=self.condition + other.condition
            if (self.condition or other.condition)
            else None,
            source_type=self.source_type,
        )

    @property
    def identifier(self) -> str:
        filters = abs(hash(str(self.condition))) if self.condition else ""
        grain = "_".join(
            [str(c.address).replace(".", "_") for c in self.grain.components]
        )
        return (
            "_join_".join([d.name for d in self.datasources])
            + (f"_at_{grain}" if grain else "_at_abstract")
            + (f"_filtered_by_{filters}" if filters else "")
        )
        # return #str(abs(hash("from_"+"_with_".join([d.name for d in self.datasources]) + ( f"_at_grain_{grain}" if grain else "" ))))

    def get_alias(
        self, concept: Concept, use_raw_name: bool = False, force_alias: bool = False
    ):
        # if we should use the raw datasource name to access
        use_raw_name = (
            True
            if (len(self.datasources) == 1 or use_raw_name) and not force_alias
            # if ((len(self.datasources) == 1 and isinstance(self.datasources[0], Datasource)) or use_raw_name) and not force_alias
            else False
        )
        for x in self.datasources:
            # query datasources should be referenced by their alias, always
            force_alias = isinstance(x, QueryDatasource)
            try:
                return x.get_alias(
                    concept.with_grain(self.grain),
                    use_raw_name,
                    force_alias=force_alias,
                )
            except ValueError as e:
                from preql.constants import logger

                logger.debug(e)
                continue
        existing = [c.with_grain(self.grain) for c in self.output_concepts]
        if concept in existing:
            return concept.name

        existing_str = [str(c) for c in existing]
        datasources = [ds.identifier for ds in self.datasources]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {str(concept)} not found on {self.identifier}; have {existing_str} from {datasources}."
        )

    @property
    def safe_location(self):
        return self.identifier


@dataclass
class Comment:
    text: str


@dataclass
class CTE:
    name: str
    source: "QueryDatasource"  # TODO: make recursive
    # output columns are what are selected/grouped by
    output_columns: List[Concept]
    source_map: Dict[str, str]
    # related columns include all referenced columns
    related_columns: List[Concept]
    grain: Grain
    base: bool = False
    group_to_grain: bool = False
    parent_ctes: List["CTE"] = field(default_factory=list)
    joins: List["Join"] = field(default_factory=list)
    condition: Optional[Union["Conditional", "Comparison"]] = None

    def __post_init__(self):
        self.output_columns = unique(self.output_columns, "address")

    def __add__(self, other: "CTE"):
        if not self.grain == other.grain:
            error = f"Attempting to merge two ctes of different grains {self.name} {other.name} grains {self.grain} {other.grain}"
            raise ValueError(error)

        self.parent_ctes = merge_ctes(self.parent_ctes + other.parent_ctes)

        self.source_map = {**self.source_map, **other.source_map}

        self.output_columns = unique(
            self.output_columns + other.output_columns, "address"
        )
        self.joins = unique(self.joins + other.joins, "unique_id")
        self.related_columns = unique(
            self.related_columns + other.related_columns, "address"
        )
        return self

    @property
    def relevant_base_ctes(self):
        """The parent CTEs includes all CTES,
        not just those immediately referenced.
        This method returns only those that are relevant
        to the output of the query."""
        return self.parent_ctes

    @property
    def base_name(self) -> str:
        # if this cte selects from a single datasource, select right from it
        if len(self.source.datasources) == 1 and isinstance(
            self.source.datasources[0], Datasource
        ):
            return self.source.datasources[0].safe_location
        # if we have multiple joined CTEs, pick the base
        # as the root
        elif self.joins and len(self.joins) > 0:
            return self.joins[0].left_cte.name
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        # return self.source_map.values()[0]
        elif self.parent_ctes:
            raise SyntaxError(
                f"{self.name} has no relevant base CTEs, {self.source_map}, {[x.name for x in self.parent_ctes]}, outputs {[x.address for x in self.output_columns]}"
            )
        return self.source.name

    @property
    def base_alias(self) -> str:
        if len(self.source.datasources) == 1 and isinstance(
            self.source.datasources[0], Datasource
        ):
            # if isinstance(self.source.datasources[0], QueryDatasource) and self.relevant_base_ctes:
            #     return self.relevant_base_ctes[0].name
            return self.source.datasources[0].full_name.replace(".", "_")
        if self.joins:
            return self.joins[0].left_cte.name
        elif self.relevant_base_ctes:
            return self.relevant_base_ctes[0].name
        elif self.parent_ctes:
            return self.parent_ctes[0].name
        return self.name

    def get_alias(self, concept: Concept) -> str:
        error = ValueError(
            f"Error: alias not found looking for alias for concept {concept}"
        )
        for cte in [self] + self.parent_ctes:
            try:
                return cte.source.get_alias(concept)
            except ValueError as e:
                if not error:
                    error = e
        return "INVALID_ALIAS"


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


@dataclass
class CompiledCTE:
    name: str
    statement: str


@dataclass
class JoinKey:
    concept: Concept

    def __str__(self):
        return str(self.concept)


@dataclass
class Join:
    left_cte: CTE
    right_cte: CTE
    jointype: JoinType
    joinkeys: List[JoinKey]

    @property
    def unique_id(self) -> str:
        return self.left_cte.name + self.right_cte.name + self.jointype.value

    def __str__(self):
        return f'{self.jointype.value} JOIN {self.left_cte.name} and {self.right_cte.name} on {",".join([str(k) for k in self.joinkeys])}'


class EnvironmentConceptDict(dict, MutableMapping[KT, VT]):
    def __getitem__(self, key, line_no=None):
        try:
            return super(EnvironmentConceptDict, self).__getitem__(key)
        except KeyError:
            matches = self._find_similar_concepts(key)
            message = f"undefined concept: {key}."
            if matches:
                message += f" Suggestions: {matches}"

            if line_no:
                raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
            raise UndefinedConceptException(message, matches)

    def _find_similar_concepts(self, concept_name):
        matches = difflib.get_close_matches(concept_name, self.keys())
        return matches


@dataclass
class Environment:
    concepts: EnvironmentConceptDict[str, Concept] = field(
        default_factory=EnvironmentConceptDict
    )
    datasources: Dict[str, Datasource] = field(default_factory=dict)
    namespace: Optional[str] = None
    working_path: str = field(default_factory=lambda: os.getcwd())


class Expr(BaseModel):
    content: Any

    def __init__(self):
        raise SyntaxError

    @property
    def input(self) -> List[Concept]:
        output: List[Concept] = []
        return output

    @property
    def safe_address(self):
        return ""

    @property
    def address(self):
        return ""


class Comparison(BaseModel):

    left: Union[
        bool,
        int,
        str,
        float,
        list,
        Function,
        Concept,
        "Conditional",
        DataType,
        "Comparison",
    ]
    right: Union[
        bool,
        int,
        str,
        float,
        list,
        Concept,
        Function,
        "Conditional",
        DataType,
        "Comparison",
    ]
    operator: ComparisonOperator

    def __add__(self, other):
        if not isinstance(other, (Comparison, Conditional)):
            raise ValueError("Cannot add Comparison to non-Comparison")
        if other == self:
            return self
        return Conditional(left=self, right=other, operator=BooleanOperator.AND)

    def __repr__(self):
        return f"{self.left} {self.operator.value} {self.right}"

    def with_namespace(self, namespace: str):
        return Comparison(
            left=self.left.with_namespace(namespace)
            if isinstance(self.left, (Concept, Function))
            else self.left,
            right=self.right.with_namespace(namespace)
            if isinstance(self.right, (Concept, Function))
            else self.right,
            operator=self.operator,
        )

    @property
    def input(self) -> List[Concept]:
        output: List[Concept] = []
        if isinstance(self.left, (Concept,)):
            output += [self.left]
        elif isinstance(self.left, (Concept, Expr, Conditional)):
            output += self.left.input
        if isinstance(self.right, (Concept,)):
            output += [self.right]
        if isinstance(self.right, (Concept, Expr, Conditional)):
            output += self.right.input
        if isinstance(self.left, Function):
            output += self.left.concept_arguments
        elif isinstance(self.right, Function):
            output += self.right.concept_arguments
        return output


class Conditional(BaseModel):
    left: Union[Concept, Comparison, "Conditional"]
    right: Union[Concept, Comparison, "Conditional"]
    operator: BooleanOperator

    def __add__(self, other) -> "Conditional":
        if other == 0:
            return self
        if not other:
            return self
        elif isinstance(other, Conditional):
            return Conditional(left=self, right=other, operator=BooleanOperator.AND)
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __repr__(self):
        return f"{self.left} {self.operator.value} {self.right}"

    def with_namespace(self, namespace: str):
        return Conditional(
            left=self.left.with_namespace(namespace),
            right=self.right.with_namespace(namespace),
            operator=self.operator,
        )

    @property
    def input(self) -> List[Concept]:
        """Return concepts directly referenced in where clause"""
        output = []
        if isinstance(self.left, Concept):
            output += self.input
        else:
            output += self.left.input
        if isinstance(self.right, Concept):
            output += self.right.input
        else:
            output += self.right.input
        if isinstance(self.left, Function):
            output += self.left.concept_arguments
        elif isinstance(self.right, Function):
            output += self.right.concept_arguments
        return output


class WhereClause(BaseModel):

    conditional: Union[Comparison, Conditional]

    @property
    def input(self) -> List[Concept]:
        return self.conditional.input

    def with_namespace(self, namespace: str):
        return WhereClause(conditional=self.conditional.with_namespace(namespace))

    @property
    def grain(self) -> Grain:
        output = []
        for item in self.input:
            if item.purpose == Purpose.KEY:
                output.append(item)
            elif item.purpose == Purpose.PROPERTY:
                output += item.grain.components if item.grain else []
        return Grain(components=list(set(output)))


# TODO: combine with CTEs
# CTE contains procesed query?
# or CTE references CTE?
@dataclass
class ProcessedQuery:
    output_columns: List[Concept]
    ctes: List[CTE]
    base: CTE
    joins: List[Join]
    grain: Grain
    limit: Optional[int] = None
    where_clause: Optional[WhereClause] = None
    order_by: Optional[OrderBy] = None
    # base:Dataset


@dataclass
class Limit:
    count: int


Concept.update_forward_refs()
Grain.update_forward_refs()
WindowItem.update_forward_refs()
WindowItemOrder.update_forward_refs()
FilterItem.update_forward_refs()
Comparison.update_forward_refs()
Conditional.update_forward_refs()
