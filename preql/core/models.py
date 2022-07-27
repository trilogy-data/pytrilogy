import os
from dataclasses import dataclass, field
from typing import List, Optional, Union, Dict, Set
from preql.utility import unique
from preql.core.enums import (
    DataType,
    Purpose,
    JoinType,
    Ordering,
    Modifier,
    FunctionType,
    BooleanOperator,
    ComparisonOperator,
)


@dataclass(eq=True, frozen=True)
class Metadata:
    pass


@dataclass(eq=True, frozen=True)
class Concept:
    name: str
    datatype: DataType
    purpose: Purpose
    metadata: Optional[Metadata] = None
    lineage: Optional["Function"] = None
    _grain: Optional["Grain"] = None
    namespace: Optional[str] = "default"

    def with_grain(self, grain: "Grain") -> "Concept":
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            _grain=grain,
            namespace=self.namespace,
        )

    @property
    def grain(self):
        if self.purpose == Purpose.KEY:
            return Grain(components=[self])
        return self._grain

    @property
    def sources(self) -> List["Concept"]:
        if self.lineage:
            output = []
            output += self.lineage.arguments
            # recursively get further lineage
            for item in self.lineage.arguments:
                output += item.sources
            return output
        return []

    @property
    def input(self):
        return [self] + self.sources


@dataclass(eq=True, frozen=True)
class ColumnAssignment:
    alias: str
    concept: Concept
    modifiers: Optional[List[Modifier]] = None

    def is_complete(self):
        return Modifier.PARTIAL not in self.modifiers


@dataclass(eq=True, frozen=True)
class Statement:
    pass


@dataclass(eq=True, frozen=True)
class Function:
    operator: FunctionType
    arguments: List[Concept]
    output_datatype: DataType
    output_purpose: Purpose
    output_grain: "Grain"
    valid_inputs: Optional[Set[DataType]] = None


@dataclass(eq=True)
class ConceptTransform:
    function: Function
    output: Concept

    @property
    def input(self) -> List[Concept]:
        return self.function.arguments


@dataclass(eq=True)
class SelectItem:
    content: Union[Concept, ConceptTransform]

    @property
    def output(self) -> Concept:
        if isinstance(self.content, ConceptTransform):
            return self.content.output
        return self.content

    @property
    def input(self) -> List[Concept]:
        return self.content.input


@dataclass(eq=True)
class OrderItem:
    expr: "Expr"
    order: Ordering


@dataclass(eq=True, frozen=True)
class OrderBy:
    items: List[OrderItem]


@dataclass(eq=True, frozen=True)
class Select:
    selection: List[SelectItem]
    where_clause: Optional["WhereClause"] = None
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None

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
            output.append(item.output)
        return output

    @property
    def all_components(self) -> List[Concept]:
        return self.input_components + self.output_components + self.grain.components

    @property
    def grain(self) -> "Grain":
        output = []
        for item in self.output_components:
            if item.purpose == Purpose.KEY:
                output.append(item)
            elif item.purpose == Purpose.PROPERTY:
                output += item.grain.components
        return Grain(components=list(set(output)))




@dataclass(eq=True, frozen=True)
class Address:
    location: str


@dataclass(frozen=True)
class Grain:
    components: List[Concept]

    def __repr__(self):
        return "Grain<" + ",".join([c.name for c in self.components]) + ">"

    @property
    def set(self):
        return set([c.name for c in self.components])

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

    def __add__(self, other: "Grain"):
        components = []
        for clist in [self.components, other.components]:
            for component in clist:
                if component in components:
                    continue
                components.append(component)
        return Grain(components=components)


@dataclass
class Datasource:
    identifier: str
    columns: List[ColumnAssignment]
    address: Address
    grain: Optional[Grain] = None
    namespace: Optional[str] = ""

    def __post_init__(self):
        # if a user skips defining a grain, use the defined keys
        if not self.grain:
            self.grain = Grain([v for v in self.concepts if v.purpose == Purpose.KEY])

    @property
    def concepts(self) -> List[Concept]:
        return [c.concept for c in self.columns]

    def get_alias(self, concept: Concept):
        for x in self.columns:
            if x.concept == concept:
                return x.alias
        existing = [c.concept.name for c in self.columns]
        raise ValueError(
            f"Concept {concept.name} not found on {self.identifier}; have {existing}."
        )


@dataclass(eq=True)
class JoinedDataSource:
    concepts:List[Concept]
    source_map: Dict[str, Datasource]
    grain: Grain
    join_paths:Dict

    @property
    def datasources(self)->List[Datasource]:
        return unique(list(self.source_map.values()), 'identifier')

    @property
    def identifier(self)->str:
        return '_join_'.join([d.identifier for d in self.datasources])

    def get_alias(self, concept: Concept):
        for x in self.datasources:
            try:
                return x.get_alias(concept)
            except ValueError:
                continue
        existing = [c.concept.name for c in self.columns]
        raise ValueError(
            f"Concept {concept.name} not found on {self.identifier}; have {existing}."
        )

    @property
    def address(self):
        return Address('TURN THIS INTO ANOTHER CTE')


@dataclass
class Comment:
    text: str


@dataclass
class CTE:
    name: str
    source: Datasource  # TODO: make recursive
    # output columns are what are selected/grouped by
    output_columns: List[Concept]
    # related columns include all referenced columns, such as filtering
    related_columns: List[Concept]
    grain: Grain
    base: bool = False
    group_to_grain: bool = False


@dataclass
class CompiledCTE:
    name: str
    statement: str


@dataclass
class JoinKey:
    inner: Concept
    outer: Concept


@dataclass
class Join:
    left_cte: CTE
    right_cte: CTE
    jointype: JoinType
    joinkeys: List[JoinKey]


@dataclass
class Environment:
    concepts: Dict[str, Concept]
    datasources: Dict[str, Datasource]
    namespace: Optional[str] = None
    working_path: str = field(default_factory=lambda: os.getcwd())


@dataclass
class Expr:
    name: str = ""

    @property
    def input(self) -> List[Concept]:
        output: List[Concept] = []
        return output


@dataclass
class Comparison:
    left: Union[Concept, Expr, "Conditional"]
    right: Union[Concept, Expr, "Conditional"]
    operator: ComparisonOperator

    @property
    def input(self) -> List[Concept]:
        output: List[Concept] = []
        if isinstance(self.left, (Concept, Expr, Conditional)):
            output += self.left.input
        if isinstance(self.right, (Concept, Expr, Conditional)):
            output += self.right.input
        return output


@dataclass
class Conditional:
    left: Union[Concept, Expr, "Conditional"]
    right: Union[Concept, Expr, "Conditional"]
    operator: BooleanOperator

    @property
    def input(self) -> List[Concept]:
        return self.left.input + self.right.input


@dataclass
class WhereClause:
    conditional: Conditional

    @property
    def input(self) -> List[Concept]:
        return self.conditional.input

    @property
    def grain(self) -> Grain:
        output = []
        for item in self.input:
            if item.purpose == Purpose.KEY:
                output.append(item)
            elif item.purpose == Purpose.PROPERTY:
                output += item.grain
        return Grain(list(set(output)))


# TODO: combine with CTEs
# CTE contains procesed query?
# or CTE references CTE?
@dataclass
class ProcessedQuery:
    output_columns: List[Concept]
    ctes: List[CTE]
    joins: List[Join]
    grain: Grain
    limit: Optional[int] = None
    where_clause: Optional[WhereClause] = None
    order_by: Optional[OrderBy] = None
    # base:Dataset

    @property
    def base(self):
        if not self.grain.components:
            return self.ctes[0]
        return [c for c in self.ctes if c.base == True][0]


@dataclass
class Limit:
    value: int


