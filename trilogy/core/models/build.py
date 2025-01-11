from trilogy.core.models.author import (
    Metadata,
    Function,
    WindowItem,
    FilterItem,
    AggregateWrapper,
    RowsetItem,
    MultiSelectLineage,
    ConceptArgs,
    Grain,
    Concept,
    get_concept_arguments,
    get_basic_type,
    CaseWhen,
    Parenthetical,
    CaseElse,
)
from trilogy.core.models.core import (
    DataType,
    ListType,
    StructType,
    MapType,
    NumericType,
    DataTyped,
    MapWrapper,
    ListWrapper,
)
from trilogy.core.environment_helpers import Environment
from pydantic import Field, BaseModel
from trilogy.core.enums import (
    Purpose,
    FunctionClass,
    Modifier,
    Derivation,
    Granularity,
    InfiniteFunctionArgs,
    DatePart,
    FunctionType,
)
from typing import Optional, Union, List, Self, Set, Tuple, Type, Sequence, Any
from trilogy.constants import DEFAULT_NAMESPACE
from functools import cached_property
from datetime import date, datetime


def get_concept_row_arguments(expr) -> List["BoundConcept"]:
    output = []
    if isinstance(expr, BoundConcept):
        output += [expr]

    elif isinstance(expr, ConceptArgs):
        output += expr.row_arguments
    return output


def get_concept_arguments(expr) -> List["BoundConcept"]:
    output = []
    if isinstance(expr, BoundConcept):
        output += [expr]

    elif isinstance(
        expr,
        ConceptArgs,
    ):
        output += expr.concept_arguments
    return output


class BoundConcept(Concept, DataTyped, ConceptArgs, BaseModel):
    name: str
    datatype: DataType | ListType | StructType | MapType | NumericType
    purpose: Purpose
    derivation: Derivation
    granularity: Granularity
    is_aggregate: bool
    metadata: Metadata = Field(
        default_factory=lambda: Metadata(description=None, line_number=None),
        validate_default=True,
    )
    lineage: Optional[
        Union[
            "BoundFunction",
            WindowItem,
            FilterItem,
            AggregateWrapper,
            RowsetItem,
            MultiSelectLineage,
        ]
    ] = None
    namespace: str
    keys: Optional[set[str]] = None
    grain: "Grain" = Field(default=None, validate_default=True)  # type: ignore
    modifiers: List[Modifier] = Field(default_factory=list)  # type: ignore
    pseudonyms: set[str] = Field(default_factory=set)

    def __init__(self, **data):
        super().__init__(**data)

    def duplicate(self) -> "BoundConcept":
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

    def set_name(self, name: str):
        self.name = name
        try:
            del self.address
        except AttributeError:
            pass

    @property
    def output(self) -> "BoundConcept":
        return self

    @cached_property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name.replace(".", "_")
        elif self.namespace:
            return f"{self.namespace.replace('.','_')}_{self.name.replace('.','_')}"
        return self.name.replace(".", "_")

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
            )  # synonym_set=generate_concept_synonyms(components))
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
        )

    def with_default_grain(self) -> "BoundConcept":
        return self._with_default_grain

    @property
    def sources(self) -> List["BoundConcept"]:
        if self.lineage:
            output: List[BoundConcept] = []

            def get_sources(
                expr: Union[
                    Function,
                    WindowItem,
                    FilterItem,
                    AggregateWrapper,
                    RowsetItem,
                    MultiSelectLineage,
                ],
                output: List[BoundConcept],
            ):
                for item in expr.concept_arguments:
                    if isinstance(item, BoundConcept):
                        if item.address == self.address:
                            raise SyntaxError(
                                f"Concept {self.address} references itself"
                            )
                        output.append(item)
                        output += item.sources

            get_sources(self.lineage, output)
            return output
        return []

    @property
    def concept_arguments(self) -> List["BoundConcept"]:
        return self.lineage.concept_arguments if self.lineage else []


class BoundFunction(Function, DataTyped, ConceptArgs, BaseModel):
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
            BoundConcept,
            "AggregateWrapper",
            "Function",
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
            "Parenthetical",
            CaseWhen,
            "CaseElse",
            list,
            ListWrapper[Any],
            WindowItem,
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
    def concept_arguments(self) -> List[BoundConcept]:
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
