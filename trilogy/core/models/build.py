from __future__ import annotations

from abc import ABC
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
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    computed_field,
    field_validator,
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
    CaseElse,
    CaseWhen,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    FilterItem,
    FuncArgs,
    Function,
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
    WhereClause,
    WindowItem,
)
from trilogy.core.models.core import (
    Addressable,
    DataType,
    DataTyped,
    ListType,
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
from trilogy.utility import unique

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
        if any([c in others for c in concept.keys]):

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
) -> list[BuildConcept]:
    pconcepts = []
    for c in concepts:
        if isinstance(c, BuildConcept):
            pconcepts.append(c)
        elif environment:
            pconcepts.append(environment.concepts[c])
        else:
            raise ValueError(
                f"Unable to resolve input {c} without environment provided to concepts_to_grain call"
            )

    final: List[BuildConcept] = []
    for sub in pconcepts:
        if not concept_is_relevant(sub, pconcepts):
            continue
        final.append(sub)
    final = unique(final, "address")
    v2 = sorted(final, key=lambda x: x.name)
    return v2


class LooseBuildConceptList(BaseModel):
    concepts: Sequence[BuildConcept]

    @cached_property
    def addresses(self) -> set[str]:
        return {s.address for s in self.concepts}

    @classmethod
    def validate(cls, v):
        return cls(v)

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


class BuildGrain(BaseModel):
    components: set[str] = Field(default_factory=set)
    where_clause: Optional[BuildWhereClause] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def without_condition(self):
        return BuildGrain(components=self.components)

    @classmethod
    def from_concepts(
        cls,
        concepts: Iterable[BuildConcept | str],
        environment: BuildEnvironment | None = None,
        where_clause: BuildWhereClause | None = None,
    ) -> "BuildGrain":

        return BuildGrain(
            components={
                c.address
                for c in concepts_to_build_grain_concepts(
                    concepts, environment=environment
                )
            },
            where_clause=where_clause,
        )

    @field_validator("components", mode="before")
    def component_validator(cls, v, info: ValidationInfo):
        output = set()
        if isinstance(v, list):
            for vc in v:
                if isinstance(vc, BuildConcept):
                    output.add(vc.address)
                else:
                    output.add(vc)
        else:
            output = v
        if not isinstance(output, set):
            raise ValueError(f"Invalid grain component {output}, is not set")
        if not all(isinstance(x, str) for x in output):
            raise ValueError(f"Invalid component {output}")
        return output

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

    def __str__(self):
        if self.abstract:
            base = "Grain<Abstract>"
        else:
            base = "Grain<" + ",".join([c for c in sorted(list(self.components))]) + ">"
        if self.where_clause:
            base += f"|{str(self.where_clause)}"
        return base

    def __radd__(self, other) -> "BuildGrain":
        if other == 0:
            return self
        else:
            return self.__add__(other)


class BuildParenthetical(DataTyped, ConstantInlineable, BuildConceptArgs, BaseModel):
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


class BuildConditional(BuildConceptArgs, ConstantInlineable, BaseModel):
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


class BuildWhereClause(BuildConceptArgs, BaseModel):
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


class BuildComparison(BuildConceptArgs, ConstantInlineable, BaseModel):

    left: Union[
        int,
        str,
        float,
        list,
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
    ]
    right: Union[
        int,
        str,
        float,
        list,
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
    ]
    operator: ComparisonOperator

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

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


class BuildSubselectComparison(BuildComparison):
    left: Union[
        int,
        str,
        float,
        list,
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
    ]
    right: Union[
        int,
        str,
        float,
        list,
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


class BuildConcept(Addressable, BuildConceptArgs, DataTyped, BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    datatype: DataType | ListType | StructType | MapType | NumericType
    purpose: Purpose
    build_is_aggregate: bool
    derivation: Derivation = Derivation.ROOT
    granularity: Granularity = Granularity.MULTI_ROW
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    metadata: Metadata = Field(
        default_factory=lambda: Metadata(description=None, line_number=None),
        validate_default=True,
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
    grain: BuildGrain = Field(default=None, validate_default=True)  # type: ignore
    modifiers: List[Modifier] = Field(default_factory=list)  # type: ignore
    pseudonyms: set[str] = Field(default_factory=set)

    def with_select_context(self, *args, **kwargs):
        return self

    @property
    def is_aggregate(self) -> bool:
        return self.build_is_aggregate

    def duplicate(self) -> BuildConcept:
        return self.model_copy(deep=True)

    def __hash__(self):
        return hash(
            f"{self.name}+{self.datatype}+ {self.purpose} + {str(self.lineage)} + {self.namespace} + {str(self.grain)} + {str(self.keys)}"
        )

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
        return self.__class__.model_construct(
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
        return self.__class__.model_construct(
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


class BuildOrderItem(DataTyped, BuildConceptArgs, BaseModel):
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


class BuildWindowItem(DataTyped, BuildConceptArgs, BaseModel):
    type: WindowType
    content: BuildConcept
    order_by: List[BuildOrderItem]
    over: List["BuildConcept"] = Field(default_factory=list)
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


class BuildCaseWhen(BuildConceptArgs, BaseModel):
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


class BuildCaseElse(BuildConceptArgs, BaseModel):
    expr: "BuildExpr"
    # this ensures that it's easily differentiable from CaseWhen
    discriminant: ComparisonOperator = ComparisonOperator.ELSE

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)


class BuildFunction(DataTyped, BuildConceptArgs, BaseModel):
    # class BuildFunction(Function):
    operator: FunctionType
    arg_count: int = Field(default=1)
    output_datatype: (
        DataType | ListType | StructType | MapType | NumericType | TraitDataType
    )
    output_purpose: Purpose
    valid_inputs: Optional[
        Union[
            Set[DataType],
            List[Set[DataType]],
        ]
    ] = None
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
            ListType,
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

    def __repr__(self):
        return f'{self.operator.value}({",".join([str(a) for a in self.arguments])})'

    def __str__(self):
        return self.__repr__()

    @property
    def datatype(self):
        return self.output_datatype

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        base = []
        for arg in self.arguments:
            base += get_concept_arguments(arg)
        return base

    @property
    def output_grain(self):
        # aggregates have an abstract grain
        base_grain = BuildGrain(components=[])
        if self.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return base_grain
        # scalars have implicit grain of all arguments
        for input in self.concept_arguments:
            base_grain += input.grain
        return base_grain


class BuildAggregateWrapper(BuildConceptArgs, DataTyped, BaseModel):
    function: BuildFunction
    by: List[BuildConcept] = Field(default_factory=list)

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


class BuildFilterItem(BuildConceptArgs, BaseModel):
    content: BuildConcept
    where: BuildWhereClause

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def output_purpose(self):
        return self.content.purpose

    @property
    def concept_arguments(self):
        return [self.content] + self.where.concept_arguments


class BuildRowsetLineage(BuildConceptArgs, BaseModel):
    name: str
    derived_concepts: List[str]
    select: SelectLineage | MultiSelectLineage


class BuildRowsetItem(DataTyped, BuildConceptArgs, BaseModel):
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


class BuildOrderBy(BaseModel):
    items: List[BuildOrderItem]

    @property
    def concept_arguments(self):
        return [x.expr for x in self.items]


class BuildAlignClause(BaseModel):
    items: List[BuildAlignItem]


class BuildSelectLineage(BaseModel):
    selection: List[BuildConcept]
    hidden_components: set[str]
    local_concepts: dict[str, BuildConcept]
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    grain: BuildGrain = Field(default_factory=BuildGrain)
    where_clause: BuildWhereClause | None = Field(default=None)
    having_clause: BuildHavingClause | None = Field(default=None)

    @property
    def output_components(self) -> List[BuildConcept]:
        return self.selection


class BuildMultiSelectLineage(BuildConceptArgs, BaseModel):
    selects: List[SelectLineage]
    grain: BuildGrain
    align: BuildAlignClause
    namespace: str
    order_by: Optional[BuildOrderBy] = None
    limit: Optional[int] = None
    where_clause: Union["BuildWhereClause", None] = Field(default=None)
    having_clause: Union["BuildHavingClause", None] = Field(default=None)
    local_concepts: dict[str, BuildConcept]
    build_concept_arguments: list[BuildConcept]
    build_output_components: list[BuildConcept]
    hidden_components: set[str]

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


class BuildAlignItem(BaseModel):
    alias: str
    concepts: List[BuildConcept]
    namespace: str = Field(default=DEFAULT_NAMESPACE, validate_default=True)

    @computed_field  # type: ignore
    @cached_property
    def concepts_lcl(self) -> LooseBuildConceptList:
        return LooseBuildConceptList(concepts=self.concepts)

    @property
    def aligned_concept(self) -> str:
        return f"{self.namespace}.{self.alias}"


class BuildColumnAssignment(BaseModel):
    alias: str | RawColumnExpr | BuildFunction
    concept: BuildConcept
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers


class BuildDatasource(BaseModel):
    name: str
    columns: List[BuildColumnAssignment]
    address: Union[Address, str]
    grain: BuildGrain = Field(
        default_factory=lambda: BuildGrain(components=set()), validate_default=True
    )
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    metadata: DatasourceMetadata = Field(
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
    ) -> Optional[str | RawColumnExpr] | BuildFunction:
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

BuildConcept.model_rebuild()


class Factory:

    def __init__(
        self,
        environment: Environment,
        local_concepts: dict[str, BuildConcept] | None = None,
        grain: Grain | None = None,
    ):
        self.grain = grain or Grain()
        self.environment = environment
        self.local_concepts: dict[str, BuildConcept] = (
            {} if local_concepts is None else local_concepts
        )

    @singledispatchmethod
    def build(self, base):
        raise NotImplementedError("Cannot build {}".format(type(base)))

    @build.register
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
        return base

    @build.register
    def _(self, base: None) -> None:
        return base

    @build.register
    def _(self, base: Function) -> BuildFunction:
        from trilogy.parsing.common import arbitrary_to_concept

        raw_args: list[Concept | FuncArgs] = []
        for arg in base.arguments:
            # to do proper discovery, we need to inject virtual intermediate ocncepts
            if isinstance(arg, (AggregateWrapper, FilterItem, WindowItem)):
                narg = arbitrary_to_concept(
                    arg,
                    environment=self.environment,
                )
                raw_args.append(narg)
            else:
                raw_args.append(arg)
        new = BuildFunction.model_construct(
            operator=base.operator,
            arguments=[self.build(c) for c in raw_args],
            output_datatype=base.output_datatype,
            output_purpose=base.output_purpose,
            valid_inputs=base.valid_inputs,
            arg_count=base.arg_count,
        )
        return new

    @build.register
    def _(self, base: ConceptRef) -> BuildConcept:
        if base.address in self.local_concepts:
            full = self.local_concepts[base.address]
            if isinstance(full, BuildConcept):
                return full
        raw = self.environment.concepts[base.address]
        return self.build(raw)

    @build.register
    def _(self, base: CaseWhen) -> BuildCaseWhen:
        return BuildCaseWhen.model_construct(
            comparison=self.build(base.comparison),
            expr=(self.build(base.expr)),
        )

    @build.register
    def _(self, base: CaseElse) -> BuildCaseElse:
        return BuildCaseElse.model_construct(expr=self.build(base.expr))

    @build.register
    def _(self, base: Concept) -> BuildConcept:
        if base.address in self.local_concepts:
            return self.local_concepts[base.address]
        new_lineage, final_grain, _ = base.get_select_grain_and_keys(
            self.grain, self.environment
        )
        build_lineage = self.build(new_lineage)
        derivation = Concept.calculate_derivation(build_lineage, base.purpose)
        granularity = Concept.calculate_granularity(
            derivation, final_grain, build_lineage
        )
        is_aggregate = Concept.calculate_is_aggregate(build_lineage)
        rval = BuildConcept.model_construct(
            name=base.name,
            datatype=base.datatype,
            purpose=base.purpose,
            metadata=base.metadata,
            lineage=build_lineage,
            grain=self.build(final_grain),
            namespace=base.namespace,
            keys=base.keys,
            modifiers=base.modifiers,
            pseudonyms=base.pseudonyms,
            ## instantiated values
            derivation=derivation,
            granularity=granularity,
            build_is_aggregate=is_aggregate,
        )
        return rval

    @build.register
    def _(self, base: AggregateWrapper) -> BuildAggregateWrapper:

        if not base.by:
            by = [
                self.build(self.environment.concepts[c]) for c in self.grain.components
            ]
        else:
            by = [self.build(x) for x in base.by]
        parent = self.build(base.function)
        return BuildAggregateWrapper.model_construct(function=parent, by=by)

    @build.register
    def _(self, base: ColumnAssignment) -> BuildColumnAssignment:

        return BuildColumnAssignment.model_construct(
            alias=(
                self.build(base.alias)
                if isinstance(base.alias, Function)
                else base.alias
            ),
            concept=self.build(
                self.environment.concepts[base.concept.address].with_grain(self.grain)
            ),
            modifiers=base.modifiers,
        )

    @build.register
    def _(self, base: OrderBy) -> BuildOrderBy:
        return BuildOrderBy.model_construct(items=[self.build(x) for x in base.items])

    @build.register
    def _(self, base: OrderItem) -> BuildOrderItem:
        return BuildOrderItem.model_construct(
            expr=(self.build(base.expr)),
            order=base.order,
        )

    @build.register
    def _(self, base: WhereClause) -> BuildWhereClause:
        return BuildWhereClause.model_construct(
            conditional=self.build(base.conditional)
        )

    @build.register
    def _(self, base: HavingClause) -> BuildHavingClause:
        return BuildHavingClause.model_construct(
            conditional=self.build(base.conditional)
        )

    @build.register
    def _(self, base: WindowItem) -> BuildWindowItem:

        return BuildWindowItem.model_construct(
            type=base.type,
            content=self.build(base.content),
            order_by=[self.build(x) for x in base.order_by],
            over=[self.build(x) for x in base.over],
            index=base.index,
        )

    @build.register
    def _(self, base: Conditional) -> BuildConditional:
        return BuildConditional.model_construct(
            left=(self.build(base.left)),
            right=(self.build(base.right)),
            operator=base.operator,
        )

    @build.register
    def _(self, base: SubselectComparison) -> BuildSubselectComparison:
        return BuildSubselectComparison.model_construct(
            left=(self.build(base.left)),
            right=(self.build(base.right)),
            operator=base.operator,
        )

    @build.register
    def _(self, base: Comparison) -> BuildComparison:
        from trilogy.parsing.common import arbitrary_to_concept

        left = base.left
        if isinstance(left, AggregateWrapper):
            left_c = arbitrary_to_concept(
                left,
                environment=self.environment,
            )
            left = left_c  # type: ignore
        right = base.right
        if isinstance(right, AggregateWrapper):
            right_c = arbitrary_to_concept(
                right,
                environment=self.environment,
            )

            right = right_c  # type: ignore
        return BuildComparison.model_construct(
            left=(self.build(left)),
            right=(self.build(right)),
            operator=base.operator,
        )

    @build.register
    def _(self, base: AlignItem) -> BuildAlignItem:
        return BuildAlignItem.model_construct(
            alias=base.alias,
            concepts=[self.build(x) for x in base.concepts],
            namespace=base.namespace,
        )

    @build.register
    def _(self, base: AlignClause) -> BuildAlignClause:
        return BuildAlignClause.model_construct(
            items=[self.build(x) for x in base.items]
        )

    @build.register
    def _(self, base: RowsetItem) -> BuildRowsetItem:

        factory = Factory(
            environment=self.environment,
            local_concepts={},
            grain=base.rowset.select.grain,
        )
        return BuildRowsetItem(
            content=factory.build(base.content), rowset=factory.build(base.rowset)
        )

    @build.register
    def _(self, base: RowsetLineage) -> BuildRowsetLineage:
        out = BuildRowsetLineage(
            name=base.name,
            derived_concepts=[x.address for x in base.derived_concepts],
            select=base.select,
        )
        return out

    @build.register
    def _(self, base: Grain) -> BuildGrain:
        if base.where_clause:
            factory = Factory(environment=self.environment)
            where = factory.build(base.where_clause)
        else:
            where = None
        return BuildGrain.model_construct(
            components=base.components, where_clause=where
        )

    @build.register
    def _(self, base: TupleWrapper) -> TupleWrapper:
        return TupleWrapper(val=[self.build(x) for x in base.val], type=base.type)

    @build.register
    def _(self, base: FilterItem) -> BuildFilterItem:
        return BuildFilterItem.model_construct(
            content=self.build(base.content), where=self.build(base.where)
        )

    @build.register
    def _(self, base: Parenthetical) -> BuildParenthetical:
        return BuildParenthetical.model_construct(content=(self.build(base.content)))

    @build.register
    def _(self, base: SelectLineage) -> BuildSelectLineage:

        from trilogy.core.models.build import (
            BuildSelectLineage,
            Factory,
        )

        materialized: dict[str, BuildConcept] = {}
        factory = Factory(
            grain=base.grain, environment=self.environment, local_concepts=materialized
        )
        for k, v in base.local_concepts.items():
            materialized[k] = factory.build(v)
        where_factory = Factory(
            grain=Grain(), environment=self.environment, local_concepts={}
        )

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
            where_clause=(
                where_factory.build(base.where_clause) if base.where_clause else None
            ),
        )

    @build.register
    def _(self, base: MultiSelectLineage) -> BuildMultiSelectLineage:

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
            x = BuildConcept.model_construct(
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
        )
        where_factory = Factory(environment=self.environment)
        lineage = BuildMultiSelectLineage.model_construct(
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
        from trilogy.core.models.build_environment import BuildEnvironment

        new = BuildEnvironment(
            namespace=base.namespace,
            cte_name_map=base.cte_name_map,
        )

        for k, v in base.concepts.items():
            new.concepts[k] = self.build(v)
        for (
            k,
            d,
        ) in base.datasources.items():
            new.datasources[k] = self.build(d)
        for k, a in base.alias_origin_lookup.items():
            new.alias_origin_lookup[k] = self.build(a)
        new.gen_concept_list_caches()
        return new

    @build.register
    def _(self, base: TraitDataType):
        return base

    @build.register
    def _(self, base: Datasource):
        local_cache: dict[str, BuildConcept] = {}
        factory = Factory(
            grain=base.grain, environment=self.environment, local_concepts=local_cache
        )
        return BuildDatasource.model_construct(
            name=base.name,
            columns=[factory.build(c) for c in base.columns],
            address=base.address,
            grain=factory.build(base.grain),
            namespace=base.namespace,
            metadata=base.metadata,
            where=(factory.build(base.where) if base.where else None),
            non_partial_for=(
                factory.build(base.non_partial_for) if base.non_partial_for else None
            ),
        )
