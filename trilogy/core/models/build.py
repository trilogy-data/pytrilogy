from __future__ import annotations

from abc import ABC
from datetime import date, datetime
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
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
    computed_field,
)

from trilogy.constants import DEFAULT_NAMESPACE, MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    Modifier,
    Ordering,
    Purpose,
    WindowOrder,
    WindowType,
)
from trilogy.core.models.author import (
    SelectContext,
    AggregateWrapper,
    Concept,
    ConceptArgs,
    FilterItem,
    Function,
    Grain,
    LooseConceptList,
    Metadata,
    MultiSelectLineage,
    SelectLineage,
    OrderItem,
    OrderBy,
    WhereClause,
    HavingClause,
    RowsetItem,
    CaseElse,
    CaseWhen, 
    Parenthetical,
    AlignClause,
    WindowItem,
    get_concept_arguments,
    get_concept_row_arguments,
)
from trilogy.core.models.environment import Environment
from trilogy.core.models.core import (
    DataType,
    DataTyped,
    ListType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TupleWrapper,
    arg_to_datatype,
    is_compatible_datatype,
)
from trilogy.utility import unique

# TODO: refactor to avoid these
if TYPE_CHECKING:
    from trilogy.core.models.execute import CTE, UnionCTE


class ConstantInlineable(ABC):
    def inline_concept(self, concept: BuildConcept):
        raise NotImplementedError


def address_with_namespace(address: str, namespace: str) -> str:
    if address.split(".", 1)[0] == DEFAULT_NAMESPACE:
        return f"{namespace}.{address.split('.',1)[1]}"
    return f"{namespace}.{address}"


class BuildParenthetical(
    DataTyped,
    ConceptArgs,
    ConstantInlineable,
    BaseModel,
):
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
        elif isinstance(x, ConceptArgs):
            base += x.concept_arguments
        return base

    @property
    def row_arguments(self) -> List[BuildConcept]:
        if isinstance(self.content, ConceptArgs):
            return self.content.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> list[tuple["BuildConcept", ...]]:
        if isinstance(self.content, ConceptArgs):
            return self.content.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)


class BuildConditional(ConceptArgs, ConstantInlineable, BaseModel):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        BuildConcept,
        BuildComparison,
        "BuildConditional",
        "BuildParenthetical",
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
        "BuildConditional",
        "BuildParenthetical",
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
    def existence_arguments(self) -> list[tuple["BuildConcept", ...]]:
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
                if isinstance(val, BuildConditional):
                    chunks.extend(val.decompose())
                else:
                    chunks.append(val)
        else:
            chunks.append(self)
        return chunks


class BuildWhereClause(ConceptArgs, BaseModel):
    conditional: Union[
        BuildSubselectComparison,
        BuildComparison,
        BuildConditional,
        "BuildParenthetical",
    ]

    def __repr__(self):
        return str(self.conditional)

    def __str__(self):
        return self.__repr__()

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.conditional.concept_arguments

    @property
    def row_arguments(self) -> List[BuildConcept]:
        return self.conditional.row_arguments

    @property
    def existence_arguments(self) -> list[tuple["BuildConcept", ...]]:
        return self.conditional.existence_arguments


class BuildHavingClause(BuildWhereClause):
    pass


class BuildComparison(ConceptArgs, ConstantInlineable, BaseModel):
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

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if self.operator in (ComparisonOperator.IS, ComparisonOperator.IS_NOT):
            if self.right != MagicConstants.NULL and DataType.BOOL != arg_to_datatype(
                self.right
            ):
                raise SyntaxError(
                    f"Cannot use {self.operator.value} with non-null or boolean value {self.right}"
                )
        elif self.operator in (ComparisonOperator.IN, ComparisonOperator.NOT_IN):
            right = arg_to_datatype(self.right)
            if not isinstance(self.right, BuildConcept) and not isinstance(
                right, ListType
            ):
                raise SyntaxError(
                    f"Cannot use {self.operator.value} with non-list type {right} in {str(self)}"
                )

            elif isinstance(right, ListType) and not is_compatible_datatype(
                arg_to_datatype(self.left), right.value_data_type
            ):
                raise SyntaxError(
                    f"Cannot compare {arg_to_datatype(self.left)} and {right} with operator {self.operator} in {str(self)}"
                )
            elif isinstance(self.right, BuildConcept) and not is_compatible_datatype(
                arg_to_datatype(self.left), arg_to_datatype(self.right)
            ):
                raise SyntaxError(
                    f"Cannot compare {arg_to_datatype(self.left)} and {arg_to_datatype(self.right)} with operator {self.operator} in {str(self)}"
                )
        else:
            if not is_compatible_datatype(
                arg_to_datatype(self.left), arg_to_datatype(self.right)
            ):
                raise SyntaxError(
                    f"Cannot compare {arg_to_datatype(self.left)} and {arg_to_datatype(self.right)} of different types with operator {self.operator} in {str(self)}"
                )

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(
            other, (BuildComparison, BuildConditional, BuildParenthetical)
        ):
            raise ValueError("Cannot add Comparison to non-Comparison")
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
        if isinstance(self.left, ConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, ConceptArgs):
            output += self.right.existence_arguments
        return output


class BuildSubselectComparison(BuildComparison):
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


class BuildConcept(Concept, BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )
    build_derivation: Derivation
    build_granularity: Granularity
    build_is_aggregate: bool
    lineage: Optional[
        Union[ 
            BuildFunction,
            Function,
            BuildWindowItem,
            WindowItem,
            FilterItem,
            BuildFilterItem,
            AggregateWrapper,
            BuildAggregateWrapper,
            RowsetItem,
            MultiSelectLineage,
        ]
    ] = None

    @classmethod
    def build(cls, base: Concept, grain:Grain, environment:Environment, local_concepts) -> BuildConcept:

        new_lineage, final_grain, keys = base.get_select_grain_and_keys(
            grain, environment
        )
        if isinstance(new_lineage, SelectContext):
            new_lineage = new_lineage.with_select_context(
                local_concepts=local_concepts, grain=grain, environment=environment
            )
        
        derivation = Concept.calculate_derivation(new_lineage, base.purpose)
        granularity = Concept.calculate_granularity(derivation, final_grain, new_lineage)
        is_aggregate = Concept.calculate_is_aggregate(new_lineage)
        return BuildConcept(
            name=base.name,
            datatype=base.datatype,
            purpose=base.purpose,
            metadata=base.metadata,
            lineage=new_lineage,
            grain=final_grain,
            namespace=base.namespace,
            keys=base.keys,
            modifiers=base.modifiers,
            pseudonyms=(environment.concepts.get(base.address) or base).pseudonyms,
            ## instantiated values
            build_derivation=derivation,
            build_granularity=granularity,
            build_is_aggregate=is_aggregate,
        )

    @property
    def derivation(self) -> Derivation:
        return self.build_derivation

    @property
    def granularity(self) -> Granularity:
        return self.build_granularity

    def with_merge(self, source: Self, target: Self, modifiers: List[Modifier]) -> Self:
        if self.address == source.address:
            new = target.with_grain(self.grain.with_merge(source, target, modifiers))
            new.pseudonyms.add(self.address)
            return new
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=(
                self.lineage.with_merge(source, target, modifiers)
                if self.lineage
                else None
            ),
            grain=self.grain.with_merge(source, target, modifiers),
            namespace=self.namespace,
            keys=(
                set(x if x != source.address else target.address for x in self.keys)
                if self.keys
                else None
            ),
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
            # build
            build_derivation=self.build_derivation,
            build_granularity=self.build_granularity,
            build_is_aggregate=self.build_is_aggregate,
        )

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

    def with_grain(self, grain: Optional["Grain"] = None) -> Self:
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage,
            grain=grain if grain else Grain(components=set()),
            namespace=self.namespace,
            keys=self.keys,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
            ## bound
            build_derivation=self.build_derivation,
            build_granularity=self.build_granularity,
            build_is_aggregate=self.build_is_aggregate,
        )

    @property
    def _with_default_grain(self) -> Self:
        if self.purpose == Purpose.KEY:
            # we need to make this abstract
            grain = Grain(components={self.address})
        elif self.purpose == Purpose.PROPERTY:
            components = []
            if self.keys:
                components = [*self.keys]
            if self.lineage:
                for item in self.lineage.concept_arguments:
                    components += [x.address for x in item.sources]
            # TODO: set synonyms
            grain = Grain(
                components=set([x for x in components]),
            )  # synonym_set=generate_BuildConcept_synonyms(components))
        elif self.purpose == Purpose.METRIC:
            grain = Grain()
        elif self.purpose == Purpose.CONSTANT:
            if self.derivation != Derivation.CONSTANT:
                grain = Grain(components={self.address})
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
            build_derivation=self.build_derivation,
            build_granularity=self.build_granularity,
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


class BuildOrderItem(BaseModel):
    expr: BuildConcept
    order: Ordering

    @property
    def output(self):
        return self.expr.output


class BuildWindowItem(DataTyped, ConceptArgs, BaseModel):
    type: WindowType
    content: BuildConcept
    order_by: List[BuildOrderItem | OrderItem]
    over: List["BuildConcept"] = Field(default_factory=list)
    index: Optional[int] = None

    def __repr__(self) -> str:
        return f"{self.type}({self.content} {self.index}, {self.over}, {self.order_by})"

    @property
    def concept_arguments(self) -> List[BuildConcept]:
        return self.arguments

    @property
    def arguments(self) -> List[BuildConcept]:
        output = [self.content]
        for order in self.order_by:
            output += [order.output]
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


def get_basic_type(
    type: DataType | ListType | StructType | MapType | NumericType,
) -> DataType:
    if isinstance(type, ListType):
        return DataType.LIST
    if isinstance(type, StructType):
        return DataType.STRUCT
    if isinstance(type, MapType):
        return DataType.MAP
    if isinstance(type, NumericType):
        return DataType.NUMERIC
    return type


class BuildCaseWhen(ConceptArgs, BaseModel):
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


class BuildCaseElse(ConceptArgs, BaseModel):
    expr: "BuildExpr"
    # this ensures that it's easily differentiable from CaseWhen
    discriminant: ComparisonOperator = ComparisonOperator.ELSE

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)


# class BuildFunction(DataTyped, ConceptArgs, BaseModel):
class BuildFunction(Function):
    operator: FunctionType
    arg_count: int = Field(default=1)
    output_datatype: DataType | ListType | StructType | MapType | NumericType
    output_purpose: Purpose
    valid_inputs: Optional[
        Union[
            Set[DataType],
            List[Set[DataType]],
        ]
    ] = None
    arguments: Sequence[
        Union[
            BuildConcept,
            "BuildAggregateWrapper",
            "BuildFunction",
            int,
            float,
            str,
            date,
            datetime,
            MapWrapper[Any, Any],
            DataType,
            ListType,
            MapType,
            NumericType,
            DatePart,
            Parenthetical, 
            CaseWhen, 
            CaseElse,
            "BuildParenthetical",
            BuildCaseWhen,
            "BuildCaseElse",
            list,
            ListWrapper[Any],
            BuildWindowItem,
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
        base_grain = Grain(components=[])
        if self.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return base_grain
        # scalars have implicit grain of all arguments
        for input in self.concept_arguments:
            base_grain += input.grain
        return base_grain


class BuildAggregateWrapper(ConceptArgs, BaseModel):
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

    @property
    def arguments(self):
        return self.function.arguments


class BuildFilterItem(ConceptArgs, BaseModel):
    content: BuildConcept
    where: "WhereClause"

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


class BuildRowsetLineage(BaseModel):
    name: str
    derived_concepts: List[BuildConcept]
    select: BuildSelectLineage | BuildMultiSelectLineage


class BuildRowsetItem(ConceptArgs, BaseModel):
    content: BuildConcept
    rowset: BuildRowsetLineage
    where: Optional["BuildWhereClause"] = None

    def __repr__(self):
        return (
            f"<Rowset<{self.rowset.name}>: {str(self.content)} where {str(self.where)}>"
        )

    def __str__(self):
        return self.__repr__()

    @property
    def arguments(self) -> List[BuildConcept]:
        return self.concept_arguments

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
        if self.where:
            return [self.content] + self.where.concept_arguments
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
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    grain: Grain = Field(default_factory=Grain)
    where_clause: Union["WhereClause", None] = Field(default=None)
    having_clause: Union["HavingClause", None] = Field(default=None)

    @property
    def output_components(self) -> List[BuildConcept]:
        return self.selection


class BuildMultiSelectLineage(ConceptArgs, BaseModel):
    selects: List[SelectLineage]
    align: AlignClause
    namespace: str
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    where_clause: Union["WhereClause", None] = Field(default=None)
    having_clause: Union["HavingClause", None] = Field(default=None)
    local_concepts: dict[str, BuildConcept]
    hidden_components: set[str]

    @property
    def grain(self):
        base = Grain()
        for select in self.selects:
            base += select.grain
        return base

    @property
    def output_components(self) -> list[BuildConcept]:
        output = [self.local_concepts[x] for x in self.derived_concept]
        for select in self.selects:
            output += select.output_components
        return [x for x in output if x.address not in self.hidden_components]

    @property
    def derived_concept(self) -> set[str]:
        output = set()
        for item in self.align.items:
            output.add(item.aligned_concept)
        return output

    @property
    def concept_arguments(self):
        output = []
        for select in self.selects:
            output += select.output_components
        return unique(output, "address")

    # these are needed to help disambiguate between parents
    def get_merge_concept(self, check: BuildConcept) -> str | None:
        for item in self.align.items:
            if check in item.concepts_lcl:
                return f"{item.namespace}.{item.alias}"
        return None

    def find_source(
        self, BuildConcept: BuildConcept, cte: CTE | UnionCTE
    ) -> BuildConcept:
        for x in self.align.items:
            if BuildConcept.name == x.alias:
                for c in x.concepts:
                    if c.address in cte.output_lcl:
                        return c
        raise SyntaxError(
            f"Could not find upstream map for multiselect {str(BuildConcept)} on cte ({cte})"
        )


class BuildLooseConceptList(BaseModel):
    BuildConcepts: List[BuildConcept]

    @cached_property
    def addresses(self) -> set[str]:
        return {s.address for s in self.BuildConcepts}

    @classmethod
    def validate(cls, v):
        return cls(v)

    def __str__(self) -> str:
        return f"lcl{str(self.addresses)}"

    def __iter__(self):
        return iter(self.BuildConcepts)

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
        if not isinstance(other, BuildConcept):
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


class BuildAlignItem(BaseModel):
    alias: str
    concepts: List[BuildConcept]
    namespace: str = Field(default=DEFAULT_NAMESPACE, validate_default=True)

    @computed_field  # type: ignore
    @cached_property
    def concepts_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.concepts)

    @property
    def aligned_concept(self) -> str:
        return f"{self.namespace}.{self.alias}"


class BuildMetadata(BaseModel):
    """Metadata container object.
    TODO: support arbitrary tags"""

    description: Optional[str] = None
    line_number: Optional[int] = None
    concept_source: ConceptSource = ConceptSource.MANUAL


class BuildWindow(BaseModel):
    count: int
    window_order: WindowOrder

    def __str__(self):
        return f"Window<{self.window_order}>"


class BuildWindowItemOver(BaseModel):
    contents: List[BuildConcept]


class BuildWindowItemOrder(BaseModel):
    contents: List["BuildOrderItem"]


class BuildComment(BaseModel):
    text: str


BuildExpr = (
    bool
    | MagicConstants
    | int
    | str
    | float
    | list
    | BuildWindowItem
    | BuildFilterItem
    | BuildConcept
    | BuildComparison
    | BuildConditional
    | BuildParenthetical
    | BuildFunction
    | BuildAggregateWrapper
)

BuildConcept.model_rebuild()
