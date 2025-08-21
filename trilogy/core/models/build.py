from __future__ import annotations

from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import cached_property, singledispatchmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Optional,
    Self,
    Sequence,
    Set,
    Tuple,
    Union,
)

from pydantic import (
    ConfigDict,
)

from trilogy.constants import DEFAULT_NAMESPACE, MagicConstants
from trilogy.core.constants import ALL_ROWS_CONCEPT
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    Modifier,
    Ordering,
    Purpose,
    WindowType,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    AlignItem,
    ArgBinding,
    CaseElse,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    FuncArgs,
    Function,
    FunctionCallWrapper,
    Grain,
    HavingClause,
    Metadata,
    MultiSelectLineage,
    OrderBy,
    OrderItem,
    Parenthetical,
    RowsetItem,
    RowsetLineage,
    SelectLineage,
    SubselectComparison,
    UndefinedConcept,
    WhereClause,
    WindowItem,
)
from trilogy.core.models.core import (
    Addressable,
    ArrayType,
    DataType,
    DataTyped,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TraitDataType,
    TupleWrapper,
    arg_to_datatype,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    DatasourceMetadata,
    RawColumnExpr,
)
from trilogy.core.models.environment import Environment

# TODO: refactor to avoid these
if TYPE_CHECKING:
    from trilogy.core.models.build_environment import BuildEnvironment
    from trilogy.core.models.execute import CTE, UnionCTE

LOGGER_PREFIX = "[MODELS_BUILD]"


class BuildConceptArgs(ABC):
    @property
    def concept_arguments(self) -> Sequence["BuildConcept"]:
        raise NotImplementedError

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        return []

    @property
    def row_arguments(self) -> Sequence["BuildConcept"]:
        return self.concept_arguments


def concept_is_relevant(
    concept: BuildConcept,
    others: list[BuildConcept],
) -> bool:

    if concept.is_aggregate and not (
        isinstance(concept.lineage, BuildAggregateWrapper) and concept.lineage.by
    ):

        return False
    if concept.purpose in (Purpose.PROPERTY, Purpose.METRIC) and concept.keys:
        if all([c in others for c in concept.keys]):
            return False
    if (
        concept.purpose == Purpose.KEY
        and concept.keys
        and all([c in others and c != concept.address for c in concept.keys])
    ):
        return False
    if concept.purpose in (Purpose.METRIC,):
        if all([c in others for c in concept.grain.components]):
            return False
    if concept.derivation in (Derivation.BASIC,):

        return any(concept_is_relevant(c, others) for c in concept.concept_arguments)
    if concept.granularity == Granularity.SINGLE_ROW:
        return False
    return True


def concepts_to_build_grain_concepts(
    concepts: Iterable[BuildConcept | str], environment: "BuildEnvironment" | None
) -> set[str]:
    pconcepts: list[BuildConcept] = []
    for c in concepts:
        if isinstance(c, BuildConcept):
            pconcepts.append(c)
        elif environment:
            pconcepts.append(environment.concepts[c])

        else:
            raise ValueError(
                f"Unable to resolve input {c} without environment provided to concepts_to_grain call"
            )

    final: set[str] = set()
    for sub in pconcepts:
        if not concept_is_relevant(sub, pconcepts):
            continue
        final.add(sub.address)

    return final


@dataclass
class LooseBuildConceptList:
    concepts: Sequence[BuildConcept]

    @cached_property
    def addresses(self) -> set[str]:
        return {s.address for s in self.concepts}

    @cached_property
    def sorted_addresses(self) -> List[str]:
        return sorted(list(self.addresses))

    def __str__(self) -> str:
        return f"lcl{str(self.sorted_addresses)}"

    def __iter__(self):
        return iter(self.concepts)

    def __eq__(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses == other.addresses

    def issubset(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses.issubset(other.addresses)

    def __contains__(self, other):
        if isinstance(other, str):
            return other in self.addresses
        if not isinstance(other, BuildConcept):
            return False
        return other.address in self.addresses

    def difference(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses.difference(other.addresses)

    def isdisjoint(self, other):
        if not isinstance(other, LooseBuildConceptList):
            return False
        return self.addresses.isdisjoint(other.addresses)


class ConstantInlineable(ABC):

    def inline_constant(self, concept: BuildConcept):
        raise NotImplementedError


def get_concept_row_arguments(expr) -> List["BuildConcept"]:
    output = []
    if isinstance(expr, BuildConcept):
        output += [expr]

    elif isinstance(expr, BuildConceptArgs):
        output += expr.row_arguments
    return output


def get_concept_arguments(expr) -> List["BuildConcept"]:
    output = []
    if isinstance(expr, BuildConcept):
        output += [expr]

    elif isinstance(
        expr,
        BuildConceptArgs,
    ):
        output += expr.concept_arguments
    return output


@dataclass
class BuildParamaterizedConceptReference(DataTyped):
    concept: BuildConcept

    def __str__(self):
        return f":{self.concept.address}"

    @property
    def safe_address(self) -> str:
        return self.concept.safe_address

    @property
    def output_datatype(self) -> DataType:
        return self.concept.output_datatype


@dataclass
class BuildGrain:
    components: set[str] = field(default_factory=set)
    where_clause: Optional[BuildWhereClause] = None
    _str: str | None = None
    _str_no_condition: str | None = None

    def without_condition(self):
        if not self.where_clause:
            return self
        return BuildGrain(components=self.components)

    @classmethod
    def from_concepts(
        cls,
        concepts: Iterable[BuildConcept | str],
        environment: BuildEnvironment | None = None,
        where_clause: BuildWhereClause | None = None,
    ) -> "BuildGrain":

        return BuildGrain(
            components=concepts_to_build_grain_concepts(
                concepts, environment=environment
            ),
            where_clause=where_clause,
        )

    def __add__(self, other: "BuildGrain") -> "BuildGrain":
        if not other:
            return self
        where = self.where_clause
        if other.where_clause:
            if not self.where_clause:
                where = other.where_clause
            elif not other.where_clause == self.where_clause:
                where = BuildWhereClause(
                    conditional=BuildConditional(
                        left=self.where_clause.conditional,
                        right=other.where_clause.conditional,
                        operator=BooleanOperator.AND,
                    )
                )
                # raise NotImplementedError(
                #     f"Cannot merge grains with where clauses, self {self.where_clause} other {other.where_clause}"
                # )
        return BuildGrain(
            components=self.components.union(other.components), where_clause=where
        )

    def __sub__(self, other: "BuildGrain") -> "BuildGrain":
        return BuildGrain(
            components=self.components.difference(other.components),
            where_clause=self.where_clause,
        )

    @property
    def abstract(self):
        return not self.components or all(
            [c.endswith(ALL_ROWS_CONCEPT) for c in self.components]
        )

    def __eq__(self, other: object):
        if isinstance(other, list):
            if all([isinstance(c, BuildConcept) for c in other]):
                return self.components == set([c.address for c in other])
            return False
        if not isinstance(other, BuildGrain):
            return False
        if self.components == other.components:
            return True
        return False

    def issubset(self, other: "BuildGrain"):
        return self.components.issubset(other.components)

    def union(self, other: "BuildGrain"):
        addresses = self.components.union(other.components)
        return BuildGrain(components=addresses, where_clause=self.where_clause)

    def isdisjoint(self, other: "BuildGrain"):
        return self.components.isdisjoint(other.components)

    def intersection(self, other: "BuildGrain") -> "BuildGrain":
        intersection = self.components.intersection(other.components)
        return BuildGrain(components=intersection)

    def _calculate_string(self):
        if self.abstract:
            base = "Grain<Abstract>"
        else:
            base = "Grain<" + ",".join(sorted(self.components)) + ">"
        if self.where_clause:
            base += f"|{str(self.where_clause)}"
        return base

    def _calculate_string_no_condition(self):
        if self.abstract:
            base = "Grain<Abstract>"
        else:
            base = "Grain<" + ",".join(sorted(self.components)) + ">"
        return base

    @property
    def str_no_condition(self):
        if self._str_no_condition:
            return self._str_no_condition
        self._str_no_condition = self._calculate_string_no_condition()
        return self._str_no_condition

    def __str__(self):
        if self._str:
            return self._str
        self._str = self._calculate_string()
        return self._str

    def __radd__(self, other) -> "BuildGrain":
        if other == 0:
            return self
        else:
            return self.__add__(other)


@dataclass
class BuildParenthetical(DataTyped, ConstantInlineable, BuildConceptArgs):
    content: "BuildExpr"

    def __add__(self, other) -> Union["BuildParenthetical", "BuildConditional"]:
        if other is None:
            return self
        elif isinstance(other, (BuildComparison, BuildConditional, BuildParenthetical)):
            return BuildConditional(
                left=self, right=other, operator=BooleanOperator.AND
            )
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"({str(self.content)})"

    def inline_constant(self, BuildConcept: BuildConcept):
        return BuildParenthetical(
            content=(
                self.content.inline_constant(BuildConcept)
                if isinstance(self.content, ConstantInlineable)
                else self.content
            )
        )

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        base: List[BuildConcept] = []
        x = self.content
        if isinstance(x, BuildConcept):
            base += [x]
        elif isinstance(x, BuildConceptArgs):
            base += x.concept_arguments
        return base

    @property
    def row_arguments(self) -> Sequence[BuildConcept]:
        if isinstance(self.content, BuildConceptArgs):
            return self.content.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        if isinstance(self.content, BuildConceptArgs):
            return self.content.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)


@dataclass
class BuildConditional(BuildConceptArgs, ConstantInlineable):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        BuildConcept,
        BuildComparison,
        BuildConditional,
        BuildParenthetical,
        BuildSubselectComparison,
        BuildFunction,
        BuildFilterItem,
    ]
    right: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        BuildConcept,
        BuildComparison,
        BuildConditional,
        BuildParenthetical,
        BuildSubselectComparison,
        BuildFunction,
        BuildFilterItem,
    ]
    operator: BooleanOperator

    def __add__(self, other) -> "BuildConditional":
        if other is None:
            return self
        elif str(other) == str(self):
            return self
        elif isinstance(other, (BuildComparison, BuildConditional, BuildParenthetical)):
            return BuildConditional(
                left=self, right=other, operator=BooleanOperator.AND
            )
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{str(self.left)} {self.operator.value} {str(self.right)}"

    def __eq__(self, other):
        if not isinstance(other, BuildConditional):
            return False
        return (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )

    def inline_constant(self, constant: BuildConcept) -> "BuildConditional":
        assert isinstance(constant.lineage, BuildFunction)
        new_val = constant.lineage.arguments[0]
        if isinstance(self.left, ConstantInlineable):
            new_left = self.left.inline_constant(constant)
        elif (
            isinstance(self.left, BuildConcept)
            and self.left.address == constant.address
        ):
            new_left = new_val
        else:
            new_left = self.left

        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif (
            isinstance(self.right, BuildConcept)
            and self.right.address == constant.address
        ):
            new_right = new_val
        else:
            new_right = self.right

        if self.right == constant:
            new_right = new_val

        return BuildConditional(
            left=new_left,
            right=new_right,
            operator=self.operator,
        )

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        """Return BuildConcepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[BuildConcept]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> list[tuple[BuildConcept, ...]]:
        output: list[tuple[BuildConcept, ...]] = []
        if isinstance(self.left, BuildConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, BuildConceptArgs):
            output += self.right.existence_arguments
        return output

    def decompose(self):
        chunks = []
        if self.operator == BooleanOperator.AND:
            for val in [self.left, self.right]:
                if isinstance(val, BuildConditional):
                    chunks.extend(val.decompose())
                else:
                    chunks.append(val)
        else:
            chunks.append(self)
        return chunks


@dataclass
class BuildWhereClause(BuildConceptArgs):
    conditional: Union[
        BuildSubselectComparison,
        BuildComparison,
        BuildConditional,
        BuildParenthetical,
    ]

    def __eq__(self, other):
        if not isinstance(other, (BuildWhereClause, WhereClause)):
            return False
        return self.conditional == other.conditional

    def __repr__(self):
        return str(self.conditional)

    def __str__(self):
        return self.__repr__()

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.conditional.concept_arguments

    @property
    def row_arguments(self) -> Sequence[BuildConcept]:
        return self.conditional.row_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        return self.conditional.existence_arguments


class BuildHavingClause(BuildWhereClause):
    pass


@dataclass
class BuildComparison(BuildConceptArgs, ConstantInlineable):

    left: Union[
        int,
        str,
        float,
        bool,
        datetime,
        date,
        BuildFunction,
        BuildConcept,
        BuildConditional,
        DataType,
        BuildComparison,
        BuildParenthetical,
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
        ListWrapper,
        TupleWrapper,
    ]
    right: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        BuildConcept,
        BuildFunction,
        BuildConditional,
        DataType,
        BuildComparison,
        BuildParenthetical,
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
        TupleWrapper,
        ListWrapper,
    ]
    operator: ComparisonOperator

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(
            other, (BuildComparison, BuildConditional, BuildParenthetical)
        ):
            raise ValueError(f"Cannot add {type(other)} to {__class__}")
        if other == self:
            return self
        return BuildConditional(left=self, right=other, operator=BooleanOperator.AND)

    def __repr__(self):
        if isinstance(self.left, BuildConcept):
            left = self.left.address
        else:
            left = str(self.left)
        if isinstance(self.right, BuildConcept):
            right = self.right.address
        else:
            right = str(self.right)
        return f"{left} {self.operator.value} {right}"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not isinstance(other, BuildComparison):
            return False
        return (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )

    def inline_constant(self, constant: BuildConcept):
        assert isinstance(constant.lineage, BuildFunction)
        new_val = constant.lineage.arguments[0]
        if isinstance(self.left, ConstantInlineable):
            new_left = self.left.inline_constant(constant)
        elif (
            isinstance(self.left, BuildConcept)
            and self.left.address == constant.address
        ):
            new_left = new_val
        else:
            new_left = self.left
        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif (
            isinstance(self.right, BuildConcept)
            and self.right.address == constant.address
        ):
            new_right = new_val
        else:
            new_right = self.right

        return self.__class__(
            left=new_left,
            right=new_right,
            operator=self.operator,
        )

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        """Return BuildConcepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[BuildConcept]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> List[Tuple[BuildConcept, ...]]:
        """Return BuildConcepts directly referenced in where clause"""
        output: List[Tuple[BuildConcept, ...]] = []
        if isinstance(self.left, BuildConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, BuildConceptArgs):
            output += self.right.existence_arguments
        return output


@dataclass
class BuildSubselectComparison(BuildComparison):
    left: Union[
        int,
        str,
        float,
        bool,
        datetime,
        date,
        BuildFunction,
        BuildConcept,
        "BuildConditional",
        DataType,
        "BuildComparison",
        "BuildParenthetical",
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
        ListWrapper,
        TupleWrapper,
    ]
    right: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        BuildConcept,
        BuildFunction,
        "BuildConditional",
        DataType,
        "BuildComparison",
        "BuildParenthetical",
        MagicConstants,
        BuildWindowItem,
        BuildAggregateWrapper,
        TupleWrapper,
        ListWrapper,
    ]
    operator: ComparisonOperator

    def __eq__(self, other):
        if not isinstance(other, BuildSubselectComparison):
            return False

        comp = (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )
        return comp

    @property
    def row_arguments(self) -> List[BuildConcept]:
        return get_concept_row_arguments(self.left)

    @property
    def existence_arguments(self) -> list[tuple["BuildConcept", ...]]:
        return [tuple(get_concept_arguments(self.right))]


@dataclass
class BuildConcept(Addressable, BuildConceptArgs, DataTyped):
    model_config = ConfigDict(extra="forbid")
    name: str
    datatype: DataType | ArrayType | StructType | MapType | NumericType | TraitDataType
    purpose: Purpose
    build_is_aggregate: bool
    derivation: Derivation = Derivation.ROOT
    granularity: Granularity = Granularity.MULTI_ROW
    namespace: Optional[str] = field(default=DEFAULT_NAMESPACE)
    metadata: Metadata = field(
        default_factory=lambda: Metadata(description=None, line_number=None),
    )
    lineage: Optional[
        Union[
            BuildFunction,
            BuildWindowItem,
            BuildFilterItem,
            BuildAggregateWrapper,
            BuildRowsetItem,
            BuildMultiSelectLineage,
        ]
    ] = None

    keys: Optional[set[str]] = None
    grain: BuildGrain = field(default=None)  # type: ignore
    modifiers: List[Modifier] = field(default_factory=list)  # type: ignore
    pseudonyms: set[str] = field(default_factory=set)

    @property
    def is_aggregate(self) -> bool:
        return self.build_is_aggregate

    @cached_property
    def hash(self) -> int:
        return hash(
            f"{self.name}+{self.datatype}+ {self.purpose} + {str(self.lineage)} + {self.namespace} + {str(self.grain)} + {str(self.keys)}"
        )

    def __hash__(self):
        return self.hash

    def __repr__(self):
        base = f"{self.address}@{self.grain}"
        return base

    @property
    def output_datatype(self):
        return self.datatype

    def __eq__(self, other: object):
        if isinstance(other, str):
            if self.address == other:
                return True
        if not isinstance(other, (BuildConcept, Concept)):
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
        grain = str(self.grain) if self.grain else "Grain<>"
        return f"{self.namespace}.{self.name}@{grain}"

    @cached_property
    def address(self) -> str:
        return f"{self.namespace}.{self.name}"

    @property
    def output(self) -> "BuildConcept":
        return self

    @property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name.replace(".", "_")
        elif self.namespace:
            return f"{self.namespace.replace('.','_')}_{self.name.replace('.','_')}"
        return self.name.replace(".", "_")

    def with_grain(self, grain: Optional["BuildGrain" | BuildConcept] = None) -> Self:
        if isinstance(grain, BuildConcept):
            grain = BuildGrain(
                components=set(
                    [
                        grain.address,
                    ]
                )
            )
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain if grain else BuildGrain(components=set()),
            namespace=self.namespace,
            keys=self.keys,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
            ## bound
            derivation=self.derivation,
            granularity=self.granularity,
            build_is_aggregate=self.build_is_aggregate,
        )

    @property
    def _with_default_grain(self) -> Self:
        if self.purpose == Purpose.KEY:
            # we need to make this abstract
            grain = BuildGrain(components={self.address})
        elif self.purpose == Purpose.PROPERTY:
            components = []
            if self.keys:
                components = [*self.keys]
            if self.lineage:
                for item in self.lineage.concept_arguments:
                    components += [x.address for x in item.sources]
            # TODO: set synonyms
            grain = BuildGrain(
                components=set([x for x in components]),
            )  # synonym_set=generate_BuildConcept_synonyms(components))
        elif self.purpose == Purpose.METRIC:
            grain = BuildGrain()
        elif self.purpose == Purpose.CONSTANT:
            if self.derivation != Derivation.CONSTANT:
                grain = BuildGrain(components={self.address})
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
            pseudonyms=self.pseudonyms,
            ## bound
            derivation=self.derivation,
            granularity=self.granularity,
            build_is_aggregate=self.build_is_aggregate,
        )

    def with_default_grain(self) -> "BuildConcept":
        return self._with_default_grain

    @property
    def sources(self) -> List["BuildConcept"]:
        if self.lineage:
            output: List[BuildConcept] = []

            def get_sources(
                expr: Union[
                    BuildFunction,
                    BuildWindowItem,
                    BuildFilterItem,
                    BuildAggregateWrapper,
                    BuildRowsetItem,
                    BuildMultiSelectLineage,
                ],
                output: List[BuildConcept],
            ):
                if isinstance(expr, BuildMultiSelectLineage):
                    return
                for item in expr.concept_arguments:
                    if isinstance(item, BuildConcept):
                        if item.address == self.address:
                            raise SyntaxError(
                                f"BuildConcept {self.address} references itself"
                            )
                        output.append(item)
                        output += item.sources

            get_sources(self.lineage, output)
            return output
        return []

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.lineage.concept_arguments if self.lineage else []


@dataclass
class BuildOrderItem(DataTyped, BuildConceptArgs):
    expr: BuildExpr
    order: Ordering

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        base: List[BuildConcept] = []
        x = self.expr
        if isinstance(x, BuildConcept):
            base += [x]
        elif isinstance(x, BuildConceptArgs):
            base += x.concept_arguments
        return base

    @property
    def row_arguments(self) -> Sequence[BuildConcept]:
        if isinstance(self.expr, BuildConceptArgs):
            return self.expr.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["BuildConcept", ...]]:
        if isinstance(self.expr, BuildConceptArgs):
            return self.expr.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)


@dataclass
class BuildWindowItem(DataTyped, BuildConceptArgs):
    type: WindowType
    content: BuildConcept
    order_by: List[BuildOrderItem]
    over: List["BuildConcept"] = field(default_factory=list)
    index: Optional[int] = None

    def __repr__(self) -> str:
        return f"{self.type}({self.content} {self.index}, {self.over}, {self.order_by})"

    def __str__(self):
        return self.__repr__()

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        output = [self.content]
        for order in self.order_by:
            output += order.concept_arguments
        for item in self.over:
            output += [item]
        return output

    @property
    def output(self) -> BuildConcept:
        return self.content

    @property
    def output_datatype(self):
        if self.type in (WindowType.RANK, WindowType.ROW_NUMBER):
            return DataType.INTEGER
        return self.content.output_datatype

    @property
    def output_purpose(self):
        return Purpose.PROPERTY


@dataclass
class BuildCaseWhen(BuildConceptArgs):
    comparison: BuildConditional | BuildSubselectComparison | BuildComparison
    expr: "BuildExpr"

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"WHEN {str(self.comparison)} THEN {str(self.expr)}"

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.comparison) + get_concept_arguments(self.expr)

    @property
    def concept_row_arguments(self):
        return get_concept_row_arguments(self.comparison) + get_concept_row_arguments(
            self.expr
        )


@dataclass
class BuildCaseElse(BuildConceptArgs):
    expr: "BuildExpr"

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)


@dataclass
class BuildFunction(DataTyped, BuildConceptArgs):
    operator: FunctionType
    arguments: Sequence[
        Union[
            int,
            float,
            str,
            date,
            datetime,
            MapWrapper[Any, Any],
            TraitDataType,
            DataType,
            ArrayType,
            MapType,
            NumericType,
            DatePart,
            BuildConcept,
            BuildAggregateWrapper,
            BuildFunction,
            BuildWindowItem,
            BuildParenthetical,
            BuildCaseWhen,
            BuildCaseElse,
            list,
            ListWrapper[Any],
        ]
    ]
    output_data_type: (
        DataType | ArrayType | StructType | MapType | NumericType | TraitDataType
    )
    output_purpose: Purpose = field(default=Purpose.KEY)
    arg_count: int = field(default=1)
    valid_inputs: Optional[
        Union[
            Set[DataType],
            List[Set[DataType]],
        ]
    ] = None

    def __repr__(self):
        return f'{self.operator.value}({",".join([str(a) for a in self.arguments])})'

    def __str__(self):
        return self.__repr__()

    @property
    def datatype(self):
        return self.output_data_type

    @property
    def output_datatype(self):
        return self.output_data_type

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        base = []
        for arg in self.arguments:
            base += get_concept_arguments(arg)
        return base

    @property
    def output_grain(self):
        # aggregates have an abstract grain
        if self.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return BuildGrain(components=[])
        # scalars have implicit grain of all arguments
        args = set()
        for input in self.concept_arguments:
            args += input.grain.components
        return BuildGrain(components=args)


@dataclass
class BuildAggregateWrapper(BuildConceptArgs, DataTyped):
    function: BuildFunction
    by: List[BuildConcept] = field(default_factory=list)

    def __str__(self):
        grain_str = [str(c) for c in self.by] if self.by else "abstract"
        return f"{str(self.function)}<{grain_str}>"

    @property
    def datatype(self):
        return self.function.datatype

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.function.concept_arguments + self.by

    @property
    def output_datatype(self):
        return self.function.output_datatype

    @property
    def output_purpose(self):
        return self.function.output_purpose


@dataclass
class BuildFilterItem(BuildConceptArgs):
    content: "BuildExpr"
    where: BuildWhereClause

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)

    @property
    def output_purpose(self):
        return self.content.purpose

    @property
    def content_concept_arguments(self):
        if isinstance(self.content, BuildConcept):
            return [self.content]
        elif isinstance(self.content, BuildConceptArgs):
            return self.content.concept_arguments
        return []

    @property
    def concept_arguments(self):
        if isinstance(self.content, BuildConcept):
            return [self.content] + self.where.concept_arguments
        elif isinstance(self.content, BuildConceptArgs):
            return self.content.concept_arguments + self.where.concept_arguments
        return self.where.concept_arguments


@dataclass
class BuildRowsetLineage(BuildConceptArgs):
    name: str
    derived_concepts: List[str]
    select: SelectLineage | MultiSelectLineage


@dataclass
class BuildRowsetItem(DataTyped, BuildConceptArgs):
    content: BuildConcept
    rowset: BuildRowsetLineage

    def __repr__(self):
        return f"<Rowset<{self.rowset.name}>: {str(self.content)}>"

    def __str__(self):
        return self.__repr__()

    @property
    def output(self) -> BuildConcept:
        return self.content

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def output_purpose(self):
        return self.content.purpose

    @property
    def concept_arguments(self):
        return [self.content]


@dataclass
class BuildOrderBy:
    items: List[BuildOrderItem]

    @property
    def concept_arguments(self):
        return [x.expr for x in self.items]


@dataclass
class BuildAlignClause:
    items: List[BuildAlignItem]


@dataclass
class BuildSelectLineage:
    selection: List[BuildConcept]
    hidden_components: set[str]
    local_concepts: dict[str, BuildConcept]
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = field(default_factory=lambda: Metadata())
    grain: BuildGrain = field(default_factory=BuildGrain)
    where_clause: BuildWhereClause | None = field(default=None)
    having_clause: BuildHavingClause | None = field(default=None)

    @property
    def output_components(self) -> List[BuildConcept]:
        return self.selection


@dataclass
class BuildMultiSelectLineage(BuildConceptArgs):
    selects: List[SelectLineage]
    grain: BuildGrain
    align: BuildAlignClause
    namespace: str
    local_concepts: dict[str, BuildConcept]
    build_concept_arguments: list[BuildConcept]
    build_output_components: list[BuildConcept]
    hidden_components: set[str]
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    where_clause: Union["BuildWhereClause", None] = field(default=None)
    having_clause: Union["BuildHavingClause", None] = field(default=None)

    @property
    def derived_concepts(self) -> set[str]:
        output = set()
        for item in self.align.items:
            output.add(item.aligned_concept)
        return output

    @property
    def output_components(self) -> list[BuildConcept]:
        return self.build_output_components

    @property
    def derived_concept(self) -> set[str]:
        output = set()
        for item in self.align.items:
            output.add(item.aligned_concept)
        return output

    @property
    def concept_arguments(self) -> list[BuildConcept]:
        return self.build_concept_arguments

    # these are needed to help disambiguate between parents
    def get_merge_concept(self, check: BuildConcept) -> str | None:
        for item in self.align.items:
            if check in item.concepts_lcl:
                return f"{item.namespace}.{item.alias}"
        return None

    def find_source(self, concept: BuildConcept, cte: CTE | UnionCTE) -> BuildConcept:
        for x in self.align.items:
            if concept.name == x.alias:
                for c in x.concepts:
                    if c.address in cte.output_lcl:
                        return c
        raise SyntaxError(
            f"Could not find upstream map for multiselect {str(concept)} on cte ({cte})"
        )


@dataclass
class BuildAlignItem:
    alias: str
    concepts: List[BuildConcept]
    namespace: str = field(default=DEFAULT_NAMESPACE)

    @cached_property
    def concepts_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.concepts)

    @property
    def aligned_concept(self) -> str:
        return f"{self.namespace}.{self.alias}"


@dataclass
class BuildColumnAssignment:
    alias: str | RawColumnExpr | BuildFunction | BuildAggregateWrapper
    concept: BuildConcept
    modifiers: List[Modifier] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers


@dataclass
class BuildDatasource:
    name: str
    columns: List[BuildColumnAssignment]
    address: Union[Address, str]
    grain: BuildGrain = field(
        default_factory=lambda: BuildGrain(components=set()),
    )
    namespace: Optional[str] = field(default=DEFAULT_NAMESPACE)
    metadata: DatasourceMetadata = field(
        default_factory=lambda: DatasourceMetadata(freshness_concept=None)
    )
    where: Optional[BuildWhereClause] = None
    non_partial_for: Optional[BuildWhereClause] = None

    def __hash__(self):
        return self.identifier.__hash__()

    def __add__(self, other):
        if not other == self:
            raise ValueError(
                "Attempted to add two datasources that are not identical, this is not a valid operation"
            )
        return self

    @property
    def condition(self):
        return None

    @property
    def can_be_inlined(self) -> bool:
        if isinstance(self.address, Address) and self.address.is_query:
            return False
        return True

    @property
    def identifier(self) -> str:
        if not self.namespace or self.namespace == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}.{self.name}"

    @property
    def safe_identifier(self) -> str:
        return self.identifier.replace(".", "_")

    @property
    def concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns]

    @property
    def group_required(self):
        return False

    @property
    def output_concepts(self) -> List[BuildConcept]:
        return self.concepts

    @property
    def full_concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL not in c.modifiers]

    @property
    def nullable_concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns if Modifier.NULLABLE in c.modifiers]

    @property
    def hidden_concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns if Modifier.HIDDEN in c.modifiers]

    @property
    def partial_concepts(self) -> List[BuildConcept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL in c.modifiers]

    def get_alias(
        self,
        concept: BuildConcept,
        use_raw_name: bool = True,
        force_alias: bool = False,
    ) -> Optional[str | RawColumnExpr] | BuildFunction | BuildAggregateWrapper:
        # 2022-01-22
        # this logic needs to be refined.
        # if concept.lineage:
        # #     return None
        for x in self.columns:
            if x.concept == concept or x.concept.with_grain(concept.grain) == concept:
                if use_raw_name:
                    return x.alias
                return concept.safe_address
        existing = [str(c.concept) for c in self.columns]
        raise ValueError(
            f"{LOGGER_PREFIX} Concept {concept} not found on {self.identifier}; have"
            f" {existing}."
        )

    @property
    def safe_location(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address

    @property
    def output_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.output_concepts)


BuildExpr = (
    BuildWindowItem
    | BuildFilterItem
    | BuildConcept
    | BuildComparison
    | BuildConditional
    | BuildParenthetical
    | BuildFunction
    | BuildAggregateWrapper
    | bool
    | MagicConstants
    | int
    | str
    | float
    | list
)


def get_canonical_pseudonyms(environment: Environment) -> dict[str, set[str]]:
    roots: dict[str, set[str]] = defaultdict(set)
    for k, v in environment.concepts.items():
        roots[v.address].add(k)
        for x in v.pseudonyms:
            roots[v.address].add(x)
    for k, v in environment.alias_origin_lookup.items():
        lookup = environment.concepts[k].address
        roots[lookup].add(v.address)
        for x2 in v.pseudonyms:
            roots[lookup].add(x2)
    return roots


def requires_concept_nesting(
    expr,
) -> AggregateWrapper | WindowItem | FilterItem | Function | None:
    if isinstance(expr, (AggregateWrapper, WindowItem, FilterItem)):
        return expr
    if isinstance(expr, Function) and expr.operator == FunctionType.GROUP:
        # group by requires nesting
        return expr
    return None


class Factory:

    def __init__(
        self,
        environment: Environment,
        local_concepts: dict[str, BuildConcept] | None = None,
        grain: Grain | None = None,
        pseudonym_map: dict[str, set[str]] | None = None,
    ):
        self.grain = grain or Grain()
        self.environment = environment
        self.local_concepts: dict[str, BuildConcept] = (
            {} if local_concepts is None else local_concepts
        )
        self.local_non_build_concepts: dict[str, Concept] = {}
        self.pseudonym_map = pseudonym_map or get_canonical_pseudonyms(environment)
        self.build_grain = self.build(self.grain) if self.grain else None

    def instantiate_concept(
        self,
        arg: (
            AggregateWrapper
            | FunctionCallWrapper
            | WindowItem
            | FilterItem
            | Function
            | ListWrapper
            | MapWrapper
            | int
            | float
            | str
            | date
        ),
    ) -> tuple[Concept, BuildConcept]:
        from trilogy.parsing.common import arbitrary_to_concept, generate_concept_name

        name = generate_concept_name(arg)
        if name in self.local_concepts and name in self.local_non_build_concepts:
            # if we already have this concept, return it
            return self.local_non_build_concepts[name], self.local_concepts[name]
        new = arbitrary_to_concept(
            arg,
            environment=self.environment,
        )
        built = self._build_concept(new)
        self.local_concepts[name] = built
        self.local_non_build_concepts[name] = new
        return new, built

    @singledispatchmethod
    def build(self, base):
        raise NotImplementedError("Cannot build {}".format(type(base)))

    @build.register
    def _(
        self,
        base: (
            int
            | str
            | float
            | list
            | date
            | TupleWrapper
            | ListWrapper
            | MagicConstants
            | MapWrapper
            | DataType
            | DatePart
            | NumericType
        ),
    ) -> (
        int
        | str
        | float
        | list
        | date
        | TupleWrapper
        | ListWrapper
        | MagicConstants
        | MapWrapper
        | DataType
        | DatePart
        | NumericType
    ):
        return self._build_primitive(base)

    def _build_primitive(self, base):
        return base

    @build.register
    def _(self, base: None) -> None:
        return self._build_none(base)

    def _build_none(self, base):
        return base

    @build.register
    def _(self, base: Function) -> BuildFunction | BuildAggregateWrapper:
        return self._build_function(base)

    def _build_function(self, base: Function) -> BuildFunction | BuildAggregateWrapper:
        raw_args: list[Concept | FuncArgs] = []
        for arg in base.arguments:
            # to do proper discovery, we need to inject virtual intermediate concepts
            # we don't use requires_concept_nesting here by design
            if isinstance(arg, (AggregateWrapper, FilterItem, WindowItem)):
                narg, _ = self.instantiate_concept(arg)
                raw_args.append(narg)
            else:
                raw_args.append(arg)
        if base.operator == FunctionType.GROUP:
            group_base = raw_args[0]
            final_args: List[Concept | ConceptRef] = []
            if isinstance(group_base, ConceptRef):
                if group_base.address in self.environment.concepts and not isinstance(
                    self.environment.concepts[group_base.address], UndefinedConcept
                ):
                    group_base = self.environment.concepts[group_base.address]
            if (
                isinstance(group_base, Concept)
                and isinstance(group_base.lineage, AggregateWrapper)
                and not group_base.lineage.by
            ):
                arguments = raw_args[1:]
                for x in arguments:
                    if isinstance(x, (ConceptRef, Concept)):
                        final_args.append(x)
                    else:
                        # constants, etc, can be ignored for group
                        continue
                _, rval = self.instantiate_concept(
                    AggregateWrapper(
                        function=group_base.lineage.function,
                        by=final_args,
                    )
                )

                return BuildFunction(
                    operator=base.operator,
                    arguments=[
                        rval,
                        *[self.handle_constant(self.build(c)) for c in raw_args[1:]],
                    ],
                    output_data_type=base.output_datatype,
                    output_purpose=base.output_purpose,
                    valid_inputs=base.valid_inputs,
                    arg_count=base.arg_count,
                )

        new = BuildFunction(
            operator=base.operator,
            arguments=[self.handle_constant(self.build(c)) for c in raw_args],
            output_data_type=base.output_datatype,
            output_purpose=base.output_purpose,
            valid_inputs=base.valid_inputs,
            arg_count=base.arg_count,
        )
        return new

    @build.register
    def _(self, base: ConceptRef) -> BuildConcept:
        return self._build_concept_ref(base)

    def _build_concept_ref(self, base: ConceptRef) -> BuildConcept:
        if base.address in self.local_concepts:
            full = self.local_concepts[base.address]
            if isinstance(full, BuildConcept):
                return full
        if base.address in self.environment.concepts:
            raw = self.environment.concepts[base.address]
            return self._build_concept(raw)
        # this will error by design - TODO - more helpful message?
        return self._build_concept(self.environment.concepts[base.address])

    @build.register
    def _(self, base: CaseWhen) -> BuildCaseWhen:
        return self._build_case_when(base)

    def _build_case_when(self, base: CaseWhen) -> BuildCaseWhen:
        comparison = base.comparison
        expr: Concept | FuncArgs = base.expr
        validation = requires_concept_nesting(expr)
        if validation:
            expr, _ = self.instantiate_concept(validation)
        return BuildCaseWhen(
            comparison=self.build(comparison),
            expr=self.build(expr),
        )

    @build.register
    def _(self, base: CaseElse) -> BuildCaseElse:
        return self._build_case_else(base)

    def _build_case_else(self, base: CaseElse) -> BuildCaseElse:
        expr: Concept | FuncArgs = base.expr
        validation = requires_concept_nesting(expr)
        if validation:
            expr, _ = self.instantiate_concept(validation)
        return BuildCaseElse(expr=self.build(expr))

    @build.register
    def _(self, base: Concept) -> BuildConcept:
        return self._build_concept(base)

    def _build_concept(self, base: Concept) -> BuildConcept:
        # TODO: if we are using parameters, wrap it in a new model and use that in rendering
        if base.address in self.local_concepts:
            return self.local_concepts[base.address]
        new_lineage, final_grain, _ = base.get_select_grain_and_keys(
            self.grain, self.environment
        )
        if new_lineage:
            build_lineage = self.build(new_lineage)
        else:
            build_lineage = None
        derivation = Concept.calculate_derivation(build_lineage, base.purpose)

        granularity = Concept.calculate_granularity(
            derivation, final_grain, build_lineage
        )
        is_aggregate = Concept.calculate_is_aggregate(build_lineage)

        # if this is a pseudonym, we need to look up the base address
        if base.address in self.environment.alias_origin_lookup:
            lookup_address = self.environment.concepts[base.address].address
            # map only to the canonical concept, not to other merged concepts
            base_pseudonyms = {lookup_address}
        else:
            base_pseudonyms = {
                x
                for x in self.pseudonym_map.get(base.address, set())
                if x != base.address
            }
        rval = BuildConcept(
            name=base.name,
            datatype=base.datatype,
            purpose=base.purpose,
            metadata=base.metadata,
            lineage=build_lineage,
            grain=self._build_grain(final_grain),
            namespace=base.namespace,
            keys=base.keys,
            modifiers=base.modifiers,
            pseudonyms=base_pseudonyms,
            ## instantiated values
            derivation=derivation,
            granularity=granularity,
            build_is_aggregate=is_aggregate,
        )
        self.local_concepts[base.address] = rval
        return rval

    @build.register
    def _(self, base: AggregateWrapper) -> BuildAggregateWrapper:
        return self._build_aggregate_wrapper(base)

    def _build_aggregate_wrapper(self, base: AggregateWrapper) -> BuildAggregateWrapper:
        if not base.by:
            by = [
                self._build_concept(self.environment.concepts[c])
                for c in self.grain.components
            ]
        else:
            by = [self.build(x) for x in base.by]

        parent: BuildFunction = self._build_function(base.function)  # type: ignore
        return BuildAggregateWrapper(function=parent, by=by)

    @build.register
    def _(self, base: ColumnAssignment) -> BuildColumnAssignment:
        return self._build_column_assignment(base)

    def _build_column_assignment(self, base: ColumnAssignment) -> BuildColumnAssignment:
        address = base.concept.address
        fetched = (
            self._build_concept(
                self.environment.alias_origin_lookup[address]
            ).with_grain(self.build_grain)
            if address in self.environment.alias_origin_lookup
            else self._build_concept(self.environment.concepts[address]).with_grain(
                self.build_grain
            )
        )

        return BuildColumnAssignment(
            alias=(
                self._build_function(base.alias)
                if isinstance(base.alias, Function)
                else base.alias
            ),
            concept=fetched,
            modifiers=base.modifiers,
        )

    @build.register
    def _(self, base: OrderBy) -> BuildOrderBy:
        return self._build_order_by(base)

    def _build_order_by(self, base: OrderBy) -> BuildOrderBy:
        return BuildOrderBy(items=[self._build_order_item(x) for x in base.items])

    @build.register
    def _(self, base: FunctionCallWrapper) -> BuildExpr:
        return self._build_function_call_wrapper(base)

    def _build_function_call_wrapper(self, base: FunctionCallWrapper) -> BuildExpr:
        # function calls are kept around purely for the parse tree
        # so discard at the build point
        return self.build(base.content)

    @build.register
    def _(self, base: OrderItem) -> BuildOrderItem:
        return self._build_order_item(base)

    def _build_order_item(self, base: OrderItem) -> BuildOrderItem:
        bexpr: Any
        validation = requires_concept_nesting(base.expr)
        if validation:
            bexpr, _ = self.instantiate_concept(validation)
        else:
            bexpr = base.expr
        return BuildOrderItem(
            expr=(self.build(bexpr)),
            order=base.order,
        )

    @build.register
    def _(self, base: WhereClause) -> BuildWhereClause:
        return self._build_where_clause(base)

    def _build_where_clause(self, base: WhereClause) -> BuildWhereClause:
        return BuildWhereClause(conditional=self.build(base.conditional))

    @build.register
    def _(self, base: HavingClause) -> BuildHavingClause:
        return self._build_having_clause(base)

    def _build_having_clause(self, base: HavingClause) -> BuildHavingClause:
        return BuildHavingClause(conditional=self.build(base.conditional))

    @build.register
    def _(self, base: WindowItem) -> BuildWindowItem:
        return self._build_window_item(base)

    def _build_window_item(self, base: WindowItem) -> BuildWindowItem:
        content: Concept | FuncArgs = base.content
        validation = requires_concept_nesting(base.content)
        if validation:
            content, _ = self.instantiate_concept(validation)
        final_by = []
        for x in base.order_by:
            if (
                isinstance(x.expr, AggregateWrapper)
                and not x.expr.by
                and isinstance(content, (ConceptRef, Concept))
            ):
                x.expr.by = [content]
            final_by.append(x)
        return BuildWindowItem(
            type=base.type,
            content=self.build(content),
            order_by=[self.build(x) for x in final_by],
            over=[self._build_concept_ref(x) for x in base.over],
            index=base.index,
        )

    @build.register
    def _(self, base: Conditional) -> BuildConditional:
        return self._build_conditional(base)

    def _build_conditional(self, base: Conditional) -> BuildConditional:
        return BuildConditional(
            left=self.handle_constant(self.build(base.left)),
            right=self.handle_constant(self.build(base.right)),
            operator=base.operator,
        )

    @build.register
    def _(self, base: SubselectComparison) -> BuildSubselectComparison:
        return self._build_subselect_comparison(base)

    def _build_subselect_comparison(
        self, base: SubselectComparison
    ) -> BuildSubselectComparison:
        right: Any = base.right
        # this has specialized logic - include all Functions
        if isinstance(base.right, (AggregateWrapper, WindowItem, FilterItem, Function)):
            right_c, _ = self.instantiate_concept(base.right)
            right = right_c
        return BuildSubselectComparison(
            left=self.handle_constant(self.build(base.left)),
            right=self.handle_constant(self.build(right)),
            operator=base.operator,
        )

    @build.register
    def _(self, base: Comparison) -> BuildComparison:
        return self._build_comparison(base)

    def _build_comparison(self, base: Comparison) -> BuildComparison:
        left = base.left
        validation = requires_concept_nesting(base.left)
        if validation:
            left_c, _ = self.instantiate_concept(validation)
            left = left_c  # type: ignore
        right = base.right
        validation = requires_concept_nesting(base.right)
        if validation:
            right_c, _ = self.instantiate_concept(validation)
            right = right_c  # type: ignore
        return BuildComparison(
            left=self.handle_constant(self.build(left)),
            right=self.handle_constant(self.build(right)),
            operator=base.operator,
        )

    @build.register
    def _(self, base: AlignItem) -> BuildAlignItem:
        return self._build_align_item(base)

    def _build_align_item(self, base: AlignItem) -> BuildAlignItem:
        return BuildAlignItem(
            alias=base.alias,
            concepts=[self._build_concept_ref(x) for x in base.concepts],
            namespace=base.namespace,
        )

    @build.register
    def _(self, base: AlignClause) -> BuildAlignClause:
        return self._build_align_clause(base)

    def _build_align_clause(self, base: AlignClause) -> BuildAlignClause:
        return BuildAlignClause(items=[self._build_align_item(x) for x in base.items])

    @build.register
    def _(self, base: RowsetItem) -> BuildRowsetItem:
        return self._build_rowset_item(base)

    def _build_rowset_item(self, base: RowsetItem) -> BuildRowsetItem:
        factory = Factory(
            environment=self.environment,
            local_concepts={},
            grain=base.rowset.select.grain,
            pseudonym_map=self.pseudonym_map,
        )
        return BuildRowsetItem(
            content=factory._build_concept_ref(base.content),
            rowset=factory._build_rowset_lineage(base.rowset),
        )

    @build.register
    def _(self, base: RowsetLineage) -> BuildRowsetLineage:
        return self._build_rowset_lineage(base)

    def _build_rowset_lineage(self, base: RowsetLineage) -> BuildRowsetLineage:
        out = BuildRowsetLineage(
            name=base.name,
            derived_concepts=[x.address for x in base.derived_concepts],
            select=base.select,
        )
        return out

    @build.register
    def _(self, base: Grain) -> BuildGrain:
        return self._build_grain(base)

    def _build_grain(self, base: Grain) -> BuildGrain:
        if base.where_clause:
            factory = Factory(
                environment=self.environment, pseudonym_map=self.pseudonym_map
            )
            where = factory._build_where_clause(base.where_clause)
        else:
            where = None
        return BuildGrain(components=base.components, where_clause=where)

    @build.register
    def _(self, base: TupleWrapper) -> TupleWrapper:
        return self._build_tuple_wrapper(base)

    def _build_tuple_wrapper(self, base: TupleWrapper) -> TupleWrapper:
        return TupleWrapper(val=[self.build(x) for x in base.val], type=base.type)

    @build.register
    def _(self, base: FilterItem) -> BuildFilterItem:
        return self._build_filter_item(base)

    def _build_filter_item(self, base: FilterItem) -> BuildFilterItem:
        if isinstance(
            base.content, (Function, AggregateWrapper, WindowItem, FilterItem)
        ):
            _, built = self.instantiate_concept(base.content)
            return BuildFilterItem(content=built, where=self.build(base.where))
        return BuildFilterItem(
            content=self.build(base.content), where=self.build(base.where)
        )

    @build.register
    def _(self, base: Parenthetical) -> BuildParenthetical:
        return self._build_parenthetical(base)

    def _build_parenthetical(self, base: Parenthetical) -> BuildParenthetical:
        return BuildParenthetical(content=(self.build(base.content)))

    @build.register
    def _(self, base: SelectLineage) -> BuildSelectLineage:
        return self._build_select_lineage(base)

    def _build_select_lineage(self, base: SelectLineage) -> BuildSelectLineage:
        from trilogy.core.models.build import (
            BuildSelectLineage,
            Factory,
        )

        materialized: dict[str, BuildConcept] = {}
        factory = Factory(
            grain=base.grain,
            environment=self.environment,
            local_concepts=materialized,
            pseudonym_map=self.pseudonym_map,
        )
        for k, v in base.local_concepts.items():
            materialized[k] = factory.build(v)
        where_factory = Factory(
            grain=Grain(),
            environment=self.environment,
            local_concepts={},
            pseudonym_map=self.pseudonym_map,
        )
        where_clause = (
            where_factory.build(base.where_clause) if base.where_clause else None
        )
        # if the where clause derives new concepts
        # we need to ensure these are accessible from the general factory
        # post resolution
        for bk, bv in where_factory.local_concepts.items():
            # but do not override any local cahced grains
            if bk in materialized:
                continue
            materialized[bk] = bv
        final: List[BuildConcept] = []
        for original in base.selection:
            new = original
            # we don't know the grain of an aggregate at assignment time
            # so rebuild at this point in the tree
            # TODO: simplify
            if new.address in materialized:
                built = materialized[new.address]
            else:
                # Sometimes cached values here don't have the latest info
                # but we can't just use environment, as it might not have the right grain.
                built = factory.build(new)
                materialized[new.address] = built
            final.append(built)
        return BuildSelectLineage(
            selection=final,
            hidden_components=base.hidden_components,
            order_by=(factory.build(base.order_by) if base.order_by else None),
            limit=base.limit,
            meta=base.meta,
            local_concepts=materialized,
            grain=self.build(base.grain),
            having_clause=(
                factory.build(base.having_clause) if base.having_clause else None
            ),
            # this uses a different grain factory
            where_clause=where_clause,
        )

    @build.register
    def _(self, base: MultiSelectLineage) -> BuildMultiSelectLineage:
        return self._build_multi_select_lineage(base)

    def _build_multi_select_lineage(
        self, base: MultiSelectLineage
    ) -> BuildMultiSelectLineage:
        local_build_cache: dict[str, BuildConcept] = {}

        parents: list[BuildSelectLineage] = [self.build(x) for x in base.selects]
        base_local = parents[0].local_concepts

        for select in parents[1:]:
            for k, v in select.local_concepts.items():
                base_local[k] = v

        # this requires custom handling to avoid circular dependencies
        final_grain = self.build(base.grain)
        derived_base = []
        for k in base.derived_concepts:
            base_concept = self.environment.concepts[k]
            x = BuildConcept(
                name=base_concept.name,
                datatype=base_concept.datatype,
                purpose=base_concept.purpose,
                build_is_aggregate=False,
                derivation=Derivation.MULTISELECT,
                lineage=None,
                grain=final_grain,
                namespace=base_concept.namespace,
            )
            local_build_cache[k] = x
            derived_base.append(x)
        all_input: list[BuildConcept] = []
        for parent in parents:
            all_input += parent.output_components
        all_output: list[BuildConcept] = derived_base + all_input
        final: list[BuildConcept] = [
            x for x in all_output if x.address not in base.hidden_components
        ]
        factory = Factory(
            grain=base.grain,
            environment=self.environment,
            local_concepts=local_build_cache,
            pseudonym_map=self.pseudonym_map,
        )
        where_factory = Factory(
            environment=self.environment, pseudonym_map=self.pseudonym_map
        )
        lineage = BuildMultiSelectLineage(
            # we don't build selects here; they'll be built automatically in query discovery
            selects=base.selects,
            grain=final_grain,
            align=factory.build(base.align),
            # self.align.with_select_context(
            #     local_build_cache, self.grain, environment
            # ),
            namespace=base.namespace,
            hidden_components=base.hidden_components,
            order_by=factory.build(base.order_by) if base.order_by else None,
            limit=base.limit,
            where_clause=(
                where_factory.build(base.where_clause) if base.where_clause else None
            ),
            having_clause=(
                factory.build(base.having_clause) if base.having_clause else None
            ),
            local_concepts=base_local,
            build_output_components=final,
            build_concept_arguments=all_input,
        )
        for k in base.derived_concepts:
            local_build_cache[k].lineage = lineage
        return lineage

    @build.register
    def _(self, base: Environment):
        return self._build_environment(base)

    def _build_environment(self, base: Environment):
        from trilogy.core.models.build_environment import BuildEnvironment

        new = BuildEnvironment(
            namespace=base.namespace,
            cte_name_map=base.cte_name_map,
        )

        for k, v in base.concepts.items():
            new.concepts[k] = self._build_concept(v)
        for (
            k,
            d,
        ) in base.datasources.items():
            new.datasources[k] = self._build_datasource(d)
        for k, a in base.alias_origin_lookup.items():
            new.alias_origin_lookup[k] = self._build_concept(a)
        # add in anything that was built as a side-effect
        for bk, bv in self.local_concepts.items():
            if bk not in new.concepts:
                new.concepts[bk] = bv
        new.gen_concept_list_caches()
        return new

    @build.register
    def _(self, base: TraitDataType):
        return self._build_trait_data_type(base)

    def _build_trait_data_type(self, base: TraitDataType):
        return base

    @build.register
    def _(self, base: ArrayType):
        return self._build_array_type(base)

    def _build_array_type(self, base: ArrayType):
        return base

    @build.register
    def _(self, base: StructType):
        return self._build_struct_type(base)

    def _build_struct_type(self, base: StructType):
        return base

    @build.register
    def _(self, base: MapType):
        return self._build_map_type(base)

    def _build_map_type(self, base: MapType):
        return base

    @build.register
    def _(self, base: ArgBinding):
        return self._build_arg_binding(base)

    def _build_arg_binding(self, base: ArgBinding):
        return base

    @build.register
    def _(self, base: Ordering):
        return self._build_ordering(base)

    def _build_ordering(self, base: Ordering):
        return base

    @build.register
    def _(self, base: Datasource):
        return self._build_datasource(base)

    def _build_datasource(self, base: Datasource):
        local_cache: dict[str, BuildConcept] = {}
        factory = Factory(
            grain=base.grain,
            environment=self.environment,
            local_concepts=local_cache,
            pseudonym_map=self.pseudonym_map,
        )
        return BuildDatasource(
            name=base.name,
            columns=[factory._build_column_assignment(c) for c in base.columns],
            address=base.address,
            grain=factory._build_grain(base.grain),
            namespace=base.namespace,
            metadata=base.metadata,
            where=(factory.build(base.where) if base.where else None),
            non_partial_for=(
                factory.build(base.non_partial_for) if base.non_partial_for else None
            ),
        )

    def handle_constant(self, base):
        if (
            isinstance(base, BuildConcept)
            and isinstance(base.lineage, BuildFunction)
            and base.lineage.operator == FunctionType.CONSTANT
        ):
            return BuildParamaterizedConceptReference(concept=base)
        elif isinstance(base, ConceptRef):
            return self.handle_constant(self.build(base))
        return base
