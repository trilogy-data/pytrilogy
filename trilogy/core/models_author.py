from __future__ import annotations

import hashlib
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
    Type,
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
from trilogy.core.constants import ALL_ROWS_CONCEPT, INTERNAL_NAMESPACE
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    InfiniteFunctionArgs,
    Modifier,
    Ordering,
    Purpose,
    WindowOrder,
    WindowType,
)
from trilogy.core.models_core import (
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
    from trilogy.core.models_environment import Environment, EnvironmentConceptDict
    from trilogy.core.models_execute import CTE, UnionCTE


class Namespaced(ABC):
    def with_namespace(self, namespace: str):
        raise NotImplementedError


class Mergeable(ABC):
    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        raise NotImplementedError

    def hydrate_missing(self, concepts: EnvironmentConceptDict):
        return self


class SelectContext(ABC):
    def with_select_context(
        self,
        local_concepts: dict[str, Concept],
        grain: Grain,
        environment: Environment,
    ) -> Any:
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


class ConstantInlineable(ABC):
    def inline_concept(self, concept: Concept):
        raise NotImplementedError


class HasUUID(ABC):
    @property
    def uuid(self) -> str:
        return hashlib.md5(str(self).encode()).hexdigest()


class ConceptRef(BaseModel):
    address: str
    line_no: int | None = None

    def hydrate(self, environment: Environment) -> Concept:
        return environment.concepts.__getitem__(self.address, self.line_no)


def address_with_namespace(address: str, namespace: str) -> str:
    if address.split(".", 1)[0] == DEFAULT_NAMESPACE:
        return f"{namespace}.{address.split('.',1)[1]}"
    return f"{namespace}.{address}"


class Parenthetical(
    DataTyped,
    ConceptArgs,
    Mergeable,
    Namespaced,
    ConstantInlineable,
    SelectContext,
    BaseModel,
):
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

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return Parenthetical(
            content=(
                self.content.with_merge(source, target, modifiers)
                if isinstance(self.content, Mergeable)
                else self.content
            )
        )

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ):
        return Parenthetical(
            content=(
                self.content.with_select_context(local_concepts, grain, environment)
                if isinstance(self.content, SelectContext)
                else self.content
            )
        )

    def inline_constant(self, concept: Concept):
        return Parenthetical(
            content=(
                self.content.inline_constant(concept)
                if isinstance(self.content, ConstantInlineable)
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

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)


class Conditional(
    Mergeable, ConceptArgs, Namespaced, ConstantInlineable, SelectContext, BaseModel
):
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

    def __eq__(self, other):
        if not isinstance(other, Conditional):
            return False
        return (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )

    def inline_constant(self, constant: Concept) -> "Conditional":
        assert isinstance(constant.lineage, Function)
        new_val = constant.lineage.arguments[0]
        if isinstance(self.left, ConstantInlineable):
            new_left = self.left.inline_constant(constant)
        elif self.left == constant:
            new_left = new_val
        else:
            new_left = self.left

        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif self.right == constant:
            new_right = new_val
        else:
            new_right = self.right

        if self.right == constant:
            new_right = new_val

        return Conditional(
            left=new_left,
            right=new_right,
            operator=self.operator,
        )

    def with_namespace(self, namespace: str) -> "Conditional":
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

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "Conditional":
        return Conditional(
            left=(
                self.left.with_merge(source, target, modifiers)
                if isinstance(self.left, Mergeable)
                else self.left
            ),
            right=(
                self.right.with_merge(source, target, modifiers)
                if isinstance(self.right, Mergeable)
                else self.right
            ),
            operator=self.operator,
        )

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ):
        return Conditional(
            left=(
                self.left.with_select_context(local_concepts, grain, environment)
                if isinstance(self.left, SelectContext)
                else self.left
            ),
            right=(
                self.right.with_select_context(local_concepts, grain, environment)
                if isinstance(self.right, SelectContext)
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


class WhereClause(Mergeable, ConceptArgs, Namespaced, SelectContext, BaseModel):
    conditional: Union[SubselectComparison, Comparison, Conditional, "Parenthetical"]

    def __repr__(self):
        return str(self.conditional)

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

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return self.__class__(
            conditional=self.conditional.with_merge(source, target, modifiers)
        )

    def with_namespace(self, namespace: str) -> Self:
        return self.__class__(conditional=self.conditional.with_namespace(namespace))

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> Self:
        return self.__class__(
            conditional=self.conditional.with_select_context(
                local_concepts, grain, environment
            )
        )

    @property
    def components(self):
        from trilogy.core.processing.utility import decompose_condition

        return decompose_condition(self.conditional)

    @property
    def is_scalar(self):
        from trilogy.core.processing.utility import is_scalar_condition

        return is_scalar_condition(self.conditional)


class Grain(Namespaced, BaseModel):
    components: set[str] = Field(default_factory=set)
    where_clause: Optional["WhereClause"] = None

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        new_components = set()
        for c in self.components:
            if c == source.address:
                new_components.add(target.address)
            else:
                new_components.add(c)
        return Grain(components=new_components)

    @classmethod
    def from_concepts(
        cls,
        concepts: List[Concept],
        environment: Environment | None = None,
        where_clause: WhereClause | None = None,
    ) -> "Grain":
        from trilogy.parsing.common import concepts_to_grain_concepts

        return Grain(
            components={
                c.address
                for c in concepts_to_grain_concepts(concepts, environment=environment)
            },
            where_clause=where_clause,
        )

    def with_namespace(self, namespace: str) -> "Grain":
        return Grain(
            components={address_with_namespace(c, namespace) for c in self.components},
            where_clause=(
                self.where_clause.with_namespace(namespace)
                if self.where_clause
                else None
            ),
        )

    @field_validator("components", mode="before")
    def component_validator(cls, v, info: ValidationInfo):
        output = set()
        if isinstance(v, list):
            for vc in v:
                if isinstance(vc, Concept):
                    output.add(vc.address)
                elif isinstance(vc, ConceptRef):
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

    def __add__(self, other: "Grain") -> "Grain":
        where = self.where_clause
        if other.where_clause:
            if not self.where_clause:
                where = other.where_clause
            elif not other.where_clause == self.where_clause:
                where = WhereClause(
                    conditional=Conditional(
                        left=self.where_clause.conditional,
                        right=other.where_clause.conditional,
                        operator=BooleanOperator.AND,
                    )
                )
                # raise NotImplementedError(
                #     f"Cannot merge grains with where clauses, self {self.where_clause} other {other.where_clause}"
                # )
        return Grain(
            components=self.components.union(other.components), where_clause=where
        )

    def __sub__(self, other: "Grain") -> "Grain":
        return Grain(
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
            if not all([isinstance(c, Concept) for c in other]):
                return False
            return self.components == set([c.address for c in other])
        if not isinstance(other, Grain):
            return False
        if self.components == other.components:
            return True
        return False

    def issubset(self, other: "Grain"):
        return self.components.issubset(other.components)

    def union(self, other: "Grain"):
        addresses = self.components.union(other.components)
        return Grain(components=addresses, where_clause=self.where_clause)

    def isdisjoint(self, other: "Grain"):
        return self.components.isdisjoint(other.components)

    def intersection(self, other: "Grain") -> "Grain":
        intersection = self.components.intersection(other.components)
        return Grain(components=intersection)

    def __str__(self):
        if self.abstract:
            base = "Grain<Abstract>"
        else:
            base = "Grain<" + ",".join([c for c in sorted(list(self.components))]) + ">"
        if self.where_clause:
            base += f"|{str(self.where_clause)}"
        return base

    def __radd__(self, other) -> "Grain":
        if other == 0:
            return self
        else:
            return self.__add__(other)


class Comparison(
    ConceptArgs, Mergeable, Namespaced, ConstantInlineable, SelectContext, BaseModel
):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        datetime,
        date,
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
        date,
        datetime,
        Concept,
        Function,
        "Conditional",
        DataType,
        "Comparison",
        "Parenthetical",
        MagicConstants,
        WindowItem,
        AggregateWrapper,
        TupleWrapper,
    ]
    operator: ComparisonOperator

    def hydrate_missing(self, concepts: EnvironmentConceptDict):
        if isinstance(self.left, UndefinedConcept) and self.left.address in concepts:
            self.left = concepts[self.left.address]
        if isinstance(self.right, UndefinedConcept) and self.right.address in concepts:
            self.right = concepts[self.right.address]
        if isinstance(self.left, Mergeable):
            self.left.hydrate_missing(concepts)
        if isinstance(self.right, Mergeable):
            self.right.hydrate_missing(concepts)
        return self

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
            if not isinstance(self.right, Concept) and not isinstance(right, ListType):
                raise SyntaxError(
                    f"Cannot use {self.operator.value} with non-list type {right} in {str(self)}"
                )

            elif isinstance(right, ListType) and not is_compatible_datatype(
                arg_to_datatype(self.left), right.value_data_type
            ):
                raise SyntaxError(
                    f"Cannot compare {arg_to_datatype(self.left)} and {right} with operator {self.operator} in {str(self)}"
                )
            elif isinstance(self.right, Concept) and not is_compatible_datatype(
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
        if not isinstance(other, (Comparison, Conditional, Parenthetical)):
            raise ValueError("Cannot add Comparison to non-Comparison")
        if other == self:
            return self
        return Conditional(left=self, right=other, operator=BooleanOperator.AND)

    def __repr__(self):
        return f"{str(self.left)} {self.operator.value} {str(self.right)}"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not isinstance(other, Comparison):
            return False
        return (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )

    def inline_constant(self, constant: Concept):
        assert isinstance(constant.lineage, Function)
        new_val = constant.lineage.arguments[0]
        if isinstance(self.left, ConstantInlineable):
            new_left = self.left.inline_constant(constant)
        elif self.left == constant:
            new_left = new_val
        else:
            new_left = self.left

        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif self.right == constant:
            new_right = new_val
        else:
            new_right = self.right

        return self.__class__(
            left=new_left,
            right=new_right,
            operator=self.operator,
        )

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return self.__class__(
            left=(
                self.left.with_merge(source, target, modifiers)
                if isinstance(self.left, Mergeable)
                else self.left
            ),
            right=(
                self.right.with_merge(source, target, modifiers)
                if isinstance(self.right, Mergeable)
                else self.right
            ),
            operator=self.operator,
        )

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

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ):
        return self.__class__(
            left=(
                self.left.with_select_context(local_concepts, grain, environment)
                if isinstance(self.left, SelectContext)
                else self.left
            ),
            # the right side does NOT need to inherit select grain
            right=(
                self.right.with_select_context(local_concepts, grain, environment)
                if isinstance(self.right, SelectContext)
                else self.right
            ),
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
    def __eq__(self, other):
        if not isinstance(other, SubselectComparison):
            return False

        comp = (
            self.left == other.left
            and self.right == other.right
            and self.operator == other.operator
        )
        return comp

    @property
    def row_arguments(self) -> List[Concept]:
        return get_concept_arguments(self.left)

    @property
    def existence_arguments(self) -> list[tuple["Concept", ...]]:
        return [tuple(get_concept_arguments(self.right))]

    def with_select_context(
        self,
        local_concepts: dict[str, Concept],
        grain: Grain,
        environment: Environment,
    ):
        # there's no need to pass the select grain through to a subselect comparison on the right
        return self.__class__(
            left=(
                self.left.with_select_context(local_concepts, grain, environment)
                if isinstance(self.left, SelectContext)
                else self.left
            ),
            right=self.right,
            operator=self.operator,
        )


class Concept(DataTyped, Mergeable, Namespaced, SelectContext, BaseModel):
    name: str
    datatype: DataType | ListType | StructType | MapType | NumericType
    purpose: Purpose
    metadata: Metadata = Field(
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
            MultiSelectLineage,
        ]
    ] = None
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    keys: Optional[set[str]] = None
    grain: "Grain" = Field(default=None, validate_default=True)  # type: ignore
    modifiers: List[Modifier] = Field(default_factory=list)  # type: ignore
    pseudonyms: set[str] = Field(default_factory=set)
    _address_cache: str | None = None

    def __init__(self, **data):
        super().__init__(**data)

    def duplicate(self) -> Concept:
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

    @property
    def is_aggregate(self):
        if (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return True
        if (
            self.lineage
            and isinstance(self.lineage, AggregateWrapper)
            and self.lineage.function.operator
            in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return True
        return False

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
        )

    @field_validator("namespace", mode="plain")
    @classmethod
    def namespace_validation(cls, v):
        return v or DEFAULT_NAMESPACE

    @field_validator("metadata", mode="before")
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
                components={
                    f'{values.get("namespace", DEFAULT_NAMESPACE)}.{values["name"]}'
                }
            )
        elif (
            "lineage" in values
            and isinstance(values["lineage"], AggregateWrapper)
            and values["lineage"].by
        ):
            v = Grain(components={c.address for c in values["lineage"].by})
        elif not v:
            v = Grain(components=set())
        elif isinstance(v, Grain):
            pass
        elif isinstance(v, Concept):
            v = Grain(components={v.address})
        elif isinstance(v, dict):
            v = Grain.model_validate(v)
        else:
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
    def output(self) -> "Concept":
        return self

    @property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name.replace(".", "_")
        elif self.namespace:
            return f"{self.namespace.replace('.','_')}_{self.name.replace('.','_')}"
        return self.name.replace(".", "_")

    def with_namespace(self, namespace: str) -> Self:
        if namespace == self.namespace:
            return self
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=self.lineage.with_namespace(namespace) if self.lineage else None,
            grain=(
                self.grain.with_namespace(namespace)
                if self.grain
                else Grain(components=set())
            ),
            namespace=(
                namespace + "." + self.namespace
                if self.namespace
                and self.namespace != DEFAULT_NAMESPACE
                and self.namespace != namespace
                else namespace
            ),
            keys=(
                set([address_with_namespace(x, namespace) for x in self.keys])
                if self.keys
                else None
            ),
            modifiers=self.modifiers,
            pseudonyms={address_with_namespace(v, namespace) for v in self.pseudonyms},
        )

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> Concept:
        new_lineage = self.lineage
        if isinstance(self.lineage, SelectContext):
            new_lineage = self.lineage.with_select_context(
                local_concepts=local_concepts, grain=grain, environment=environment
            )
        final_grain = self.grain or grain
        keys = self.keys if self.keys else None
        if self.is_aggregate and isinstance(new_lineage, Function) and grain.components:
            grain_components = [environment.concepts[c] for c in grain.components]
            new_lineage = AggregateWrapper(function=new_lineage, by=grain_components)
            final_grain = grain
            keys = set(grain.components)
        elif (
            self.is_aggregate and not keys and isinstance(new_lineage, AggregateWrapper)
        ):
            keys = set([x.address for x in new_lineage.by])

        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=new_lineage,
            grain=final_grain,
            namespace=self.namespace,
            keys=keys,
            modifiers=self.modifiers,
            # a select needs to always defer to the environment for pseudonyms
            # TODO: evaluate if this should be cached
            pseudonyms=(environment.concepts.get(self.address) or self).pseudonyms,
        )

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
                for item in self.lineage.arguments:
                    if isinstance(item, Concept):
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
                    MultiSelectLineage,
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
    def derivation(self) -> Derivation:
        if self.lineage and isinstance(self.lineage, WindowItem):
            return Derivation.WINDOW
        elif self.lineage and isinstance(self.lineage, FilterItem):
            return Derivation.FILTER
        elif self.lineage and isinstance(self.lineage, AggregateWrapper):
            return Derivation.AGGREGATE
        elif self.lineage and isinstance(self.lineage, RowsetItem):
            return Derivation.ROWSET
        elif self.lineage and isinstance(self.lineage, MultiSelectLineage):
            return Derivation.MULTISELECT
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return Derivation.AGGREGATE
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator == FunctionType.UNNEST
        ):
            return Derivation.UNNEST
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator == FunctionType.UNION
        ):
            return Derivation.UNION
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in FunctionClass.SINGLE_ROW.value
        ):
            return Derivation.CONSTANT

        elif self.lineage and isinstance(self.lineage, Function):
            if not self.lineage.concept_arguments:
                return Derivation.CONSTANT
            elif all(
                [
                    x.derivation == Derivation.CONSTANT
                    for x in self.lineage.concept_arguments
                ]
            ):
                return Derivation.CONSTANT
            return Derivation.BASIC
        elif self.purpose == Purpose.CONSTANT:
            return Derivation.CONSTANT
        return Derivation.ROOT

    @property
    def granularity(self) -> Granularity:
        """ "used to determine if concepts need to be included in grain
        calculations"""
        if self.derivation == Derivation.CONSTANT:
            # constants are a single row
            return Granularity.SINGLE_ROW
        elif self.derivation == Derivation.AGGREGATE:
            # if it's an aggregate grouped over all rows
            # there is only one row left and it's fine to cross_join
            if all([x.endswith(ALL_ROWS_CONCEPT) for x in self.grain.components]):
                return Granularity.SINGLE_ROW
        elif self.namespace == INTERNAL_NAMESPACE and self.name == ALL_ROWS_CONCEPT:
            return Granularity.SINGLE_ROW
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in (FunctionType.UNNEST, FunctionType.UNION)
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

    def with_filter(
        self,
        condition: "Conditional | Comparison | Parenthetical",
        environment: Environment | None = None,
    ) -> "Concept":
        from trilogy.utility import string_to_hash

        if self.lineage and isinstance(self.lineage, FilterItem):
            if self.lineage.where.conditional == condition:
                return self
        hash = string_to_hash(self.name + str(condition))
        new = Concept(
            name=f"{self.name}_filter_{hash}",
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            lineage=FilterItem(content=self, where=WhereClause(conditional=condition)),
            keys=(self.keys if self.purpose == Purpose.PROPERTY else None),
            grain=self.grain if self.grain else Grain(components=set()),
            namespace=self.namespace,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
        )
        if environment:
            environment.add_concept(new)
        return new


class UndefinedConcept(Concept, Mergeable, Namespaced):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str
    line_no: int | None = None
    datatype: DataType | ListType | StructType | MapType | NumericType = (
        DataType.UNKNOWN
    )
    purpose: Purpose = Purpose.UNKNOWN

    def with_select_context(
        self,
        local_concepts: dict[str, Concept],
        grain: Grain,
        environment: Environment,
    ) -> "Concept":
        if self.address in local_concepts:
            rval = local_concepts[self.address]
            rval = rval.with_select_context(local_concepts, grain, environment)
            return rval
        if environment.concepts.fail_on_missing:
            environment.concepts.raise_undefined(self.address, line_no=self.line_no)
        return self


class ConceptTransform(Namespaced, BaseModel):
    function: Function | FilterItem | WindowItem | AggregateWrapper
    output: Concept
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def input(self) -> List[Concept]:
        return [v for v in self.function.arguments if isinstance(v, Concept)]

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return ConceptTransform(
            function=self.function.with_merge(source, target, modifiers),
            output=self.output.with_merge(source, target, modifiers),
            modifiers=self.modifiers + modifiers,
        )

    def with_namespace(self, namespace: str) -> "ConceptTransform":
        return ConceptTransform(
            function=self.function.with_namespace(namespace),
            output=self.output.with_namespace(namespace),
            modifiers=self.modifiers,
        )


class OrderItem(Mergeable, SelectContext, Namespaced, BaseModel):
    expr: Concept
    order: Ordering

    def with_namespace(self, namespace: str) -> "OrderItem":
        return OrderItem(expr=self.expr.with_namespace(namespace), order=self.order)

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> "OrderItem":
        return OrderItem(
            expr=self.expr.with_select_context(
                local_concepts, grain, environment=environment
            ),
            order=self.order,
        )

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "OrderItem":
        return OrderItem(
            expr=source.with_merge(source, target, modifiers), order=self.order
        )

    @property
    def input(self):
        return self.expr.input

    @property
    def output(self):
        return self.expr.output


class WindowItem(DataTyped, Mergeable, Namespaced, SelectContext, BaseModel):
    type: WindowType
    content: Concept
    order_by: List["OrderItem"]
    over: List["Concept"] = Field(default_factory=list)
    index: Optional[int] = None

    def __repr__(self) -> str:
        return f"{self.type}({self.content} {self.index}, {self.over}, {self.order_by})"

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "WindowItem":
        return WindowItem(
            type=self.type,
            content=self.content.with_merge(source, target, modifiers),
            over=[x.with_merge(source, target, modifiers) for x in self.over],
            order_by=[x.with_merge(source, target, modifiers) for x in self.order_by],
            index=self.index,
        )

    def with_namespace(self, namespace: str) -> "WindowItem":
        return WindowItem(
            type=self.type,
            content=self.content.with_namespace(namespace),
            over=[x.with_namespace(namespace) for x in self.over],
            order_by=[x.with_namespace(namespace) for x in self.order_by],
            index=self.index,
        )

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> "WindowItem":
        return WindowItem(
            type=self.type,
            content=self.content.with_select_context(
                local_concepts, grain, environment
            ),
            over=[
                x.with_select_context(local_concepts, grain, environment)
                for x in self.over
            ],
            order_by=[
                x.with_select_context(local_concepts, grain, environment)
                for x in self.order_by
            ],
            index=self.index,
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


class CaseWhen(Namespaced, SelectContext, BaseModel):
    comparison: Conditional | SubselectComparison | Comparison
    expr: "Expr"

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"WHEN {str(self.comparison)} THEN {str(self.expr)}"

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

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> CaseWhen:
        return CaseWhen(
            comparison=self.comparison.with_select_context(
                local_concepts, grain, environment
            ),
            expr=(
                (self.expr.with_select_context(local_concepts, grain, environment))
                if isinstance(self.expr, SelectContext)
                else self.expr
            ),
        )


class CaseElse(Namespaced, SelectContext, BaseModel):
    expr: "Expr"
    # this ensures that it's easily differentiable from CaseWhen
    discriminant: ComparisonOperator = ComparisonOperator.ELSE

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)

    def with_select_context(
        self,
        local_concepts: dict[str, Concept],
        grain: Grain,
        environment: Environment,
    ):
        return CaseElse(
            discriminant=self.discriminant,
            expr=(
                self.expr.with_select_context(local_concepts, grain, environment)
                if isinstance(
                    self.expr,
                    SelectContext,
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


class Function(DataTyped, Mergeable, Namespaced, SelectContext, BaseModel):
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
            Concept,
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

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> "Function":
        base = Function(
            operator=self.operator,
            arguments=[
                (
                    c.with_select_context(local_concepts, grain, environment)
                    if isinstance(
                        c,
                        SelectContext,
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
        return base

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
                and get_basic_type(arg.datatype.data_type) not in valid_inputs[idx]
            ):
                if arg.datatype != DataType.UNKNOWN:
                    raise TypeError(
                        f"Invalid input datatype {arg.datatype.data_type} passed into position {idx}"
                        f" for {operator_name} from concept {arg.name}, valid is {valid_inputs[idx]}"
                    )
            if (
                isinstance(arg, Function)
                and get_basic_type(arg.output_datatype) not in valid_inputs[idx]
            ):
                if arg.output_datatype != DataType.UNKNOWN:
                    raise TypeError(
                        f"Invalid input datatype {arg.output_datatype} passed into"
                        f" {operator_name} from function {arg.operator.name}, need {valid_inputs[idx]}"
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
                if (
                    isinstance(arg, ptype)
                    and get_basic_type(dtype) in valid_inputs[idx]
                ):
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

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "Function":
        return Function(
            operator=self.operator,
            arguments=[
                (
                    c.with_merge(source, target, modifiers)
                    if isinstance(
                        c,
                        Mergeable,
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


class AggregateWrapper(Mergeable, Namespaced, SelectContext, BaseModel):
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

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return AggregateWrapper(
            function=self.function.with_merge(source, target, modifiers=modifiers),
            by=(
                [c.with_merge(source, target, modifiers) for c in self.by]
                if self.by
                else []
            ),
        )

    def with_namespace(self, namespace: str) -> "AggregateWrapper":
        return AggregateWrapper(
            function=self.function.with_namespace(namespace),
            by=[c.with_namespace(namespace) for c in self.by] if self.by else [],
        )

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> "AggregateWrapper":
        if not self.by:
            by = [environment.concepts[c] for c in grain.components]
        else:
            by = [
                x.with_select_context(local_concepts, grain, environment)
                for x in self.by
            ]
        parent = self.function.with_select_context(local_concepts, grain, environment)
        return AggregateWrapper(function=parent, by=by)


class FilterItem(Namespaced, SelectContext, BaseModel):
    content: Concept
    where: "WhereClause"

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "FilterItem":
        return FilterItem(
            content=source.with_merge(source, target, modifiers),
            where=self.where.with_merge(source, target, modifiers),
        )

    def with_namespace(self, namespace: str) -> "FilterItem":
        return FilterItem(
            content=self.content.with_namespace(namespace),
            where=self.where.with_namespace(namespace),
        )

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> "FilterItem":
        return FilterItem(
            content=self.content.with_select_context(
                local_concepts, grain, environment
            ),
            where=self.where.with_select_context(local_concepts, grain, environment),
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


class RowsetLineage(Namespaced, Mergeable, BaseModel):
    name: str
    derived_concepts: List[Concept]
    select: SelectLineage | MultiSelectLineage

    def with_namespace(self, namespace: str):
        return RowsetLineage(
            name=self.name,
            derived_concepts=[
                x.with_namespace(namespace) for x in self.derived_concepts
            ],
            select=self.select.with_namespace(namespace),
        )

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return RowsetLineage(
            name=self.name,
            derived_concepts=[
                x.with_merge(source, target, modifiers) for x in self.derived_concepts
            ],
            select=self.select.with_merge(source, target, modifiers),
        )


class RowsetItem(Mergeable, Namespaced, BaseModel):
    content: Concept
    rowset: RowsetLineage
    where: Optional["WhereClause"] = None

    def __repr__(self):
        return (
            f"<Rowset<{self.rowset.name}>: {str(self.content)} where {str(self.where)}>"
        )

    def __str__(self):
        return self.__repr__()

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return RowsetItem(
            content=self.content.with_merge(source, target, modifiers),
            rowset=self.rowset,
            where=(
                self.where.with_merge(source, target, modifiers) if self.where else None
            ),
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


class OrderBy(SelectContext, Mergeable, Namespaced, BaseModel):
    items: List[OrderItem]

    def with_namespace(self, namespace: str) -> "OrderBy":
        return OrderBy(items=[x.with_namespace(namespace) for x in self.items])

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "OrderBy":
        return OrderBy(
            items=[x.with_merge(source, target, modifiers) for x in self.items]
        )

    def with_select_context(self, local_concepts, grain, environment):
        return OrderBy(
            items=[
                x.with_select_context(local_concepts, grain, environment)
                for x in self.items
            ]
        )

    @property
    def concept_arguments(self):
        return [x.expr for x in self.items]


class AlignClause(Namespaced, BaseModel):
    items: List[AlignItem]

    def with_namespace(self, namespace: str) -> "AlignClause":
        return AlignClause(items=[x.with_namespace(namespace) for x in self.items])


class SelectLineage(Mergeable, Namespaced, BaseModel):
    selection: List[SelectItem]
    local_concepts: dict[str, Concept]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    grain: Grain = Field(default_factory=Grain)
    where_clause: Union["WhereClause", None] = Field(default=None)
    having_clause: Union["HavingClause", None] = Field(default=None)

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
    def hidden_components(self) -> set[str]:
        output = set()
        for item in self.selection:
            if isinstance(item, SelectItem) and Modifier.HIDDEN in item.modifiers:
                output.add(item.output.address)
        return output

    @property
    def all_components(self) -> List[Concept]:
        return self.input_components + self.output_components

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> SelectLineage:
        return SelectLineage(
            selection=[x.with_merge(source, target, modifiers) for x in self.selection],
            local_concepts={
                x: y.with_merge(source, target, modifiers)
                for x, y in self.local_concepts.items()
            },
            order_by=(
                self.order_by.with_merge(source, target, modifiers)
                if self.order_by
                else None
            ),
            limit=self.limit,
            grain=self.grain.with_merge(source, target, modifiers),
            where_clause=(
                self.where_clause.with_merge(source, target, modifiers)
                if self.where_clause
                else None
            ),
            having_clause=(
                self.having_clause.with_merge(source, target, modifiers)
                if self.having_clause
                else None
            ),
        )


class MultiSelectLineage(Mergeable, Namespaced, BaseModel):
    selects: List[SelectLineage]
    align: AlignClause
    namespace: str
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    where_clause: Union["WhereClause", None] = Field(default=None)
    having_clause: Union["HavingClause", None] = Field(default=None)
    local_concepts: dict[str, Concept]

    @property
    def grain(self):
        base = Grain()
        for select in self.selects:
            base += select.grain
        return base

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> MultiSelectLineage:
        new = MultiSelectLineage(
            selects=[s.with_merge(source, target, modifiers) for s in self.selects],
            align=self.align,
            namespace=self.namespace,
            order_by=(
                self.order_by.with_merge(source, target, modifiers)
                if self.order_by
                else None
            ),
            limit=self.limit,
            where_clause=(
                self.where_clause.with_merge(source, target, modifiers)
                if self.where_clause
                else None
            ),
            having_clause=(
                self.having_clause.with_merge(source, target, modifiers)
                if self.having_clause
                else None
            ),
            local_concepts={
                x: y.with_merge(source, target, modifiers)
                for x, y in self.local_concepts.items()
            },
        )
        return new

    def with_namespace(self, namespace: str) -> "MultiSelectLineage":
        return MultiSelectLineage(
            selects=[c.with_namespace(namespace) for c in self.selects],
            align=self.align.with_namespace(namespace),
            namespace=namespace,
            order_by=self.order_by.with_namespace(namespace) if self.order_by else None,
            limit=self.limit,
            where_clause=(
                self.where_clause.with_namespace(namespace)
                if self.where_clause
                else None
            ),
            having_clause=(
                self.having_clause.with_namespace(namespace)
                if self.having_clause
                else None
            ),
            local_concepts={
                x: y.with_namespace(namespace) for x, y in self.local_concepts.items()
            },
        )

    @computed_field  # type: ignore
    @cached_property
    def hidden_components(self) -> set[str]:
        output: set[str] = set()
        for select in self.selects:
            output = output.union(select.hidden_components)
        return output

    @property
    def output_components(self):
        output = [self.local_concepts[x] for x in self.derived_concepts]
        for select in self.selects:
            output += select.output_components
        return output

    @property
    def arguments(self) -> List[Concept]:
        output = []
        for select in self.selects:
            output += select.input_components
        return unique(output, "address")

    @property
    def derived_concepts(self) -> set[str]:
        output = set()
        for item in self.align.items:
            output.add(item.aligned_concept)
        return output

    @property
    def concept_arguments(self):
        output = []
        for select in self.selects:
            output += select.input_components
        return unique(output, "address")

    def get_merge_concept(self, check: Concept) -> str | None:
        for item in self.align.items:
            if check in item.concepts_lcl:
                return f"{item.namespace}.{item.alias}"
        return None

    def find_source(self, concept: Concept, cte: CTE | UnionCTE) -> Concept:
        for x in self.align.items:
            if concept.name == x.alias:
                for c in x.concepts:
                    if c.address in cte.output_lcl:
                        return c
        raise SyntaxError(
            f"Could not find upstream map for multiselect {str(concept)} on cte ({cte})"
        )


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


class AlignItem(Namespaced, BaseModel):
    alias: str
    concepts: List[Concept]
    namespace: str = Field(default=DEFAULT_NAMESPACE, validate_default=True)

    @computed_field  # type: ignore
    @cached_property
    def concepts_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.concepts)

    @property
    def aligned_concept(self) -> str:
        return f"{self.namespace}.{self.alias}"

    def with_namespace(self, namespace: str) -> "AlignItem":
        return AlignItem(
            alias=self.alias,
            concepts=[c.with_namespace(namespace) for c in self.concepts],
            namespace=namespace,
        )


class Metadata(BaseModel):
    """Metadata container object.
    TODO: support arbitrary tags"""

    description: Optional[str] = None
    line_number: Optional[int] = None
    concept_source: ConceptSource = ConceptSource.MANUAL


class Window(BaseModel):
    count: int
    window_order: WindowOrder

    def __str__(self):
        return f"Window<{self.window_order}>"


class WindowItemOver(BaseModel):
    contents: List[Concept]


class WindowItemOrder(BaseModel):
    contents: List["OrderItem"]


class Comment(BaseModel):
    text: str


class HavingClause(WhereClause):
    pass

    def hydrate_missing(self, concepts: EnvironmentConceptDict):
        self.conditional.hydrate_missing(concepts)

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> HavingClause:
        return HavingClause(
            conditional=self.conditional.with_select_context(
                local_concepts, grain, environment
            )
        )


Expr = (
    bool
    | MagicConstants
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


class SelectItem(Mergeable, Namespaced, BaseModel):
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

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "SelectItem":
        return SelectItem(
            content=self.content.with_merge(source, target, modifiers),
            modifiers=modifiers,
        )

    def with_namespace(self, namespace: str) -> "SelectItem":
        return SelectItem(
            content=self.content.with_namespace(namespace),
            modifiers=self.modifiers,
        )
