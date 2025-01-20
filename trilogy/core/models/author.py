from __future__ import annotations

import hashlib
from abc import ABC
from datetime import date, datetime
from functools import cached_property
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
from trilogy.core.constants import ALL_ROWS_CONCEPT
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
    from trilogy.core.models.environment import Environment, EnvironmentConceptDict
    from trilogy.core.models.execute import CTE, UnionCTE
    from trilogy.core.models.build import BuildWhereClause, BuildHavingClause, BuildAggregateWrapper, BuildAlignClause


class Namespaced(ABC):
    def with_namespace(self, namespace: str):
        raise NotImplementedError


class Mergeable(ABC):
    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        raise NotImplementedError

    def hydrate_missing(self, concepts: EnvironmentConceptDict):
        return self


class ConstantInlineable(ABC):
    def inline_concept(self, concept: Concept):
        raise NotImplementedError


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
    def concept_arguments(self) -> Sequence["Concept"]:
        raise NotImplementedError

    @property
    def existence_arguments(self) -> Sequence[tuple["Concept", ...]]:
        return []

    @property
    def row_arguments(self) -> Sequence["Concept"]:
        return self.concept_arguments


class HasUUID(ABC):
    @property
    def uuid(self) -> str:
        return hashlib.md5(str(self).encode()).hexdigest()


class ConceptRef(Namespaced, DataTyped, SelectContext, Mergeable, BaseModel):
    address: str
    datatype: DataType | ListType | StructType | MapType | NumericType = (
        DataType.UNKNOWN
    )
    metadata: Optional["Metadata"] = None

    @property
    def line_no(self) -> int | None:
        if self.metadata:
            return self.metadata.line_number
        return None

    def __repr__(self):
        return f"ref:{self.address}"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if isinstance(other, Concept):
            return self.address == other.address
        elif isinstance(other, str):
            return self.address == other
        elif isinstance(other, ConceptRef):
            return self.address == other.address
        return False

    @property
    def namespace(self):
        return self.address.rsplit(".", 1)[0]

    @property
    def name(self):
        return self.address.rsplit(".", 1)[1]

    @property
    def output_datatype(self):
        return self.datatype

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> ConceptRef:
        if self.address == source.address:
            return ConceptRef(
                address=target.address, datatype=target.datatype, metadata=self.metadata
            )
        return self

    def with_namespace(self, namespace: str):
        return ConceptRef(
            address=address_with_namespace(self.address, namespace),
            datatype=self.datatype,
            line_no=self.line_no,
        )

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ):
        from trilogy.core.models.build import BuildConcept

        if self.address in local_concepts:
            return local_concepts[self.address]
        full = environment.concepts[self.address]
        if isinstance(full, BuildConcept):
            return full
        return full.with_select_context(local_concepts, grain, environment)


class UndefinedConcept(ConceptRef):
    pass

    @property
    def reference(self):
        return self

    @property
    def purpose(self):
        return Purpose.UNKNOWN


def address_with_namespace(address: str, namespace: str) -> str:
    ns = address.split(".", 1)[0]
    if ns == namespace:
        return address
    if ns == DEFAULT_NAMESPACE:
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

    @field_validator("content", mode="before")
    @classmethod
    def content_validator(cls, v, info: ValidationInfo):
        if isinstance(v, Concept):
            return v.reference
        return v

    def __add__(self, other) -> Union["Parenthetical", "Conditional"]:
        if other is None:
            return self
        elif isinstance(other, (Comparison, Conditional, Parenthetical)):
            return Conditional(left=self, right=other, operator=BooleanOperator.AND)
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __str__(self):
        return self.__repr__()

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
        from trilogy.core.models.build import BuildParenthetical

        return BuildParenthetical(
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
    def concept_arguments(self) -> Sequence[ConceptRef]:
        base: List[ConceptRef] = []
        x = self.content
        if isinstance(x, ConceptRef):
            base += [x]
        elif isinstance(x, ConceptArgs):
            base += x.concept_arguments
        return base

    @property
    def row_arguments(self) -> Sequence[Concept]:
        if isinstance(self.content, ConceptArgs):
            return self.content.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["Concept", ...]]:
        if isinstance(self.content, ConceptArgs):
            return self.content.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)


class Conditional(
    Mergeable, ConceptArgs, Namespaced, ConstantInlineable, SelectContext, BaseModel
):
    left: Expr
    right: Expr
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
        elif isinstance(self.left, Concept) and self.left.address == constant.address:
            new_left = new_val
        else:
            new_left = self.left

        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif isinstance(self.right, Concept) and self.right.address == constant.address:
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
        from trilogy.core.models.build import BuildConditional

        return BuildConditional(
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
    def concept_arguments(self) -> Sequence[Concept]:
        """Return concepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> Sequence[Concept]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> Sequence[tuple["Concept", ...]]:
        output: list[Concept] = []
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

    def __str__(self):
        return self.__repr__()

    @property
    def concept_arguments(self) -> List[Concept]:
        return self.conditional.concept_arguments

    @property
    def row_arguments(self) -> List[Concept]:
        return self.conditional.row_arguments

    @property
    def existence_arguments(self) -> list[tuple["Concept", ...]]:
        return self.conditional.existence_arguments

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> Self:
        return self.__class__(
            conditional=self.conditional.with_merge(source, target, modifiers)
        )

    def with_namespace(self, namespace: str) -> Self:
        return self.__class__(conditional=self.conditional.with_namespace(namespace))

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> "BuildWhereClause":
        from trilogy.core.models.build import BuildWhereClause

        return BuildWhereClause(
            conditional=self.conditional.with_select_context(
                local_concepts, grain, environment
            )
        )


class HavingClause(WhereClause):
    pass

    def hydrate_missing(self, concepts: EnvironmentConceptDict):
        self.conditional.hydrate_missing(concepts)

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> Self:
        from trilogy.core.models.build import BuildHavingClause

        return BuildHavingClause(
            conditional=self.conditional.with_select_context(
                local_concepts, grain, environment
            )
        )


class Grain(Namespaced, BaseModel):
    components: set[str] = Field(default_factory=set)
    where_clause: Optional["WhereClause"] = None

    def without_condition(self):
        return Grain(components=self.components)

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
        concepts: Iterable[Concept | str],
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
        if not other:
            return self
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
        ConceptRef,
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
        ConceptRef,
        Function,
        Conditional,
        DataType,
        Comparison,
        Parenthetical,
        MagicConstants,
        WindowItem,
        AggregateWrapper,
        TupleWrapper,
    ]
    operator: ComparisonOperator

    @field_validator("left", mode="before")
    @classmethod
    def left_validator(cls, v, info: ValidationInfo):
        if isinstance(v, Concept):
            return v.reference
        return v

    @field_validator("right", mode="before")
    @classmethod
    def right_validator(cls, v, info: ValidationInfo):
        if isinstance(v, Concept):
            return v.reference
        return v

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
            if not isinstance(
                self.right, (ConceptRef, Concept, ListType, TupleWrapper)
            ):
                raise SyntaxError(
                    f"Cannot use {self.operator.value} with non-list, non-tuple, non-concept object {right} in {str(self)}"
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
        if isinstance(self.left, Concept):
            left = self.left.address
        else:
            left = str(self.left)
        if isinstance(self.right, Concept):
            right = self.right.address
        else:
            right = str(self.right)
        return f"{left} {self.operator.value} {right}"

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
        elif (
            isinstance(self.left, ConceptRef) and self.left.address == constant.address
        ):
            new_left = new_val
        else:
            new_left = self.left
        if isinstance(self.right, ConstantInlineable):
            new_right = self.right.inline_constant(constant)
        elif (
            isinstance(self.right, ConceptRef)
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
        from trilogy.core.models.build import BuildComparison

        return BuildComparison(
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
    def concept_arguments(self) -> List[ConceptRef]:
        """Return concepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[Concept]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
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
        return get_concept_row_arguments(self.left)

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
        from trilogy.core.models.build import BuildSubselectComparison

        return BuildSubselectComparison(
            left=(
                self.left.with_select_context(local_concepts, grain, environment)
                if isinstance(self.left, SelectContext)
                else self.left
            ),
            right=(
                self.right.with_select_context(local_concepts, Grain(), environment)
                if isinstance(self.right, SelectContext)
                else self.right
            ),
            operator=self.operator,
        )


class Concept(DataTyped, ConceptArgs, Mergeable, Namespaced, SelectContext, BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )
    name: str
    datatype: DataType | ListType | StructType | MapType | NumericType
    purpose: Purpose
    derivation: Derivation = Derivation.ROOT
    granularity: Granularity = Granularity.MULTI_ROW
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

    def __init__(self, **data):
        super().__init__(**data)
        if self.name == "composite_id_alt":
            if "local.composite_id_alt" in self.pseudonyms:
                raise SyntaxError

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
    def reference(self) -> ConceptRef:
        return ConceptRef(
            address=self.address,
            datatype=self.output_datatype,
            metadata=self.metadata,
        )

    @property
    def output_datatype(self):
        return self.datatype

    @classmethod
    def calculate_is_aggregate(cls, lineage):
        if lineage and isinstance(lineage, Function):
            if lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                return True
        if (
            lineage
            and isinstance(lineage, AggregateWrapper)
            and lineage.function.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return True
        return False

    @property
    def is_aggregate(self):
        return self.calculate_is_aggregate(self.lineage)

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
            derivation=self.derivation,
            granularity=self.granularity,
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
        elif not v and values.get("purpose", None) == Purpose.PROPERTY:
            v = Grain(components=values.get("keys", set()) or set())
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
        if isinstance(other, ConceptRef):
            return self.address == other.address
        if not isinstance(other, Concept):
            return False
        return (
            self.name == other.name
            and self.datatype == other.datatype
            and self.purpose == other.purpose
            and self.namespace == other.namespace
            and self.grain == other.grain
            and self.derivation == other.derivation
            and self.granularity == other.granularity
            # and self.keys == other.keys
        )

    def __str__(self):
        grain = str(self.grain) if self.grain else "Grain<>"
        return f"{self.namespace}.{self.name}@{grain}"

    @property
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

    def with_namespace(self, namespace: str) -> Self:
        if namespace == self.namespace:
            return self
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            granularity=self.granularity,
            derivation=self.derivation,
            metadata=self.metadata,
            lineage=self.lineage.with_namespace(namespace) if self.lineage else None,
            grain=(
                self.grain.with_namespace(namespace)
                if self.grain
                else Grain(components=set())
            ),
            namespace=(
                namespace + "." + self.namespace
                if self.namespace != DEFAULT_NAMESPACE
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

    def get_select_grain_and_keys(
        self, grain: Grain, environment: Environment
    ) -> Tuple[
        Function
        | WindowItem
        | FilterItem
        | AggregateWrapper
        | RowsetItem
        | MultiSelectLineage
        | None,
        Grain,
        set[str] | None,
    ]:
        new_lineage = self.lineage
        final_grain = grain if not self.grain.components else self.grain
        keys = self.keys

        if self.is_aggregate and isinstance(new_lineage, Function) and grain.components:
            grain_components = [
                environment.concepts[c].reference for c in grain.components
            ]
            new_lineage = AggregateWrapper(function=new_lineage, by=grain_components)
            final_grain = grain
            keys = set(grain.components)
        elif isinstance(new_lineage, AggregateWrapper) and not new_lineage.by and grain:
            grain_components = [
                environment.concepts[c].reference for c in grain.components
            ]
            new_lineage = AggregateWrapper(
                function=new_lineage.function, by=grain_components
            )
            final_grain = grain
            keys = set([x.address for x in new_lineage.by])
        elif self.derivation == Derivation.BASIC:

            pkeys: set[str] = set()
            assert new_lineage
            for x_ref in new_lineage.concept_arguments:
                x = environment.concepts[x_ref]
                if isinstance(x, UndefinedConcept):
                    continue
                _, _, parent_keys = x.get_select_grain_and_keys(grain, environment)
                if parent_keys:
                    pkeys.update(parent_keys)
            raw_keys = pkeys
            # deduplicate
            final_grain = Grain.from_concepts(raw_keys, environment)
            keys = final_grain.components
        return new_lineage, final_grain, keys

    def set_select_grain(self, grain: Grain, environment: Environment) -> Self:
        """Assign a mutable concept the appropriate grain/keys for a select"""
        new_lineage, final_grain, keys = self.get_select_grain_and_keys(
            grain, environment
        )
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            granularity=self.granularity,
            derivation=self.derivation,
            metadata=self.metadata,
            lineage=new_lineage,
            grain=final_grain,
            namespace=self.namespace,
            keys=keys,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
        )

    def with_select_context(
        self,
        local_concepts: dict[str, Concept],
        grain: Grain | None,
        environment: Environment,
    ) -> Concept:
        """Propagate the select context to the lineage of the concept"""
        from trilogy.core.models.build import BuildConcept

        if isinstance(self, BuildConcept):
            if self.address not in local_concepts:
                local_concepts[self.address] = self
            return self
        if self.address in local_concepts:
            check = local_concepts[self.address]
            if not isinstance(check, BuildConcept):
                check = BuildConcept.build(check, grain, environment, local_concepts)
            return check
        new = BuildConcept.build(
            self, grain=grain, environment=environment, local_concepts=local_concepts
        )
        local_concepts[self.address] = new
        return new

    def with_grain(self, grain: Optional["Grain"] = None) -> Self:
        return self.__class__(
            name=self.name,
            datatype=self.datatype,
            purpose=self.purpose,
            metadata=self.metadata,
            granularity=self.granularity,
            derivation=self.derivation,
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
            granularity=self.granularity,
            derivation=self.derivation,
            grain=grain,
            keys=self.keys,
            namespace=self.namespace,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
        )

    def with_default_grain(self) -> "Concept":
        return self._with_default_grain

    @property
    def sources(self) -> List["ConceptRef"]:
        if self.lineage:
            output: List[ConceptRef] = []

            def get_sources(
                expr: Union[
                    Function,
                    WindowItem,
                    FilterItem,
                    AggregateWrapper,
                    RowsetItem,
                    MultiSelectLineage,
                ],
                output: List[ConceptRef],
            ):

                for item in expr.concept_arguments:
                    if isinstance(item, (ConceptRef,)):
                        if item.address == self.address:
                            raise SyntaxError(
                                f"Concept {self.address} references itself"
                            )
                        output.append(item)

                        # output += item.sources

            get_sources(self.lineage, output)
            return output
        return []

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        return self.lineage.concept_arguments if self.lineage else []

    @classmethod
    def calculate_derivation(self, lineage, purpose):
        from trilogy.core.models.build import (
            BuildAggregateWrapper,
            BuildFilterItem,
            BuildFunction,
            BuildMultiSelectLineage,
            BuildRowsetItem,
            BuildWindowItem,
        )

        if lineage and isinstance(lineage, (BuildWindowItem, WindowItem)):
            return Derivation.WINDOW
        elif lineage and isinstance(lineage, (BuildFilterItem, FilterItem)):
            return Derivation.FILTER
        elif lineage and isinstance(lineage, (BuildAggregateWrapper, AggregateWrapper)):
            return Derivation.AGGREGATE
        elif lineage and isinstance(lineage, (BuildRowsetItem, RowsetItem)):
            return Derivation.ROWSET
        elif lineage and isinstance(
            lineage, (BuildMultiSelectLineage, MultiSelectLineage)
        ):
            return Derivation.MULTISELECT
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        ):
            return Derivation.AGGREGATE
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator == FunctionType.UNNEST
        ):
            return Derivation.UNNEST
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator == FunctionType.UNION
        ):
            return Derivation.UNION
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator in FunctionClass.SINGLE_ROW.value
        ):
            return Derivation.CONSTANT

        elif lineage and isinstance(lineage, (BuildFunction, Function)):
            if not lineage.concept_arguments:
                return Derivation.CONSTANT
            elif all(
                [x.derivation == Derivation.CONSTANT for x in lineage.concept_arguments]
            ):
                return Derivation.CONSTANT
            return Derivation.BASIC
        elif purpose == Purpose.CONSTANT:
            return Derivation.CONSTANT
        return Derivation.ROOT

    # @property
    # def derivation(self) -> Derivation:
    #     return self.calculate_derivation(self.lineage, self.purpose)

    @classmethod
    def calculate_granularity(cls, derivation: Derivation, grain: Grain, lineage):
        from trilogy.core.models.build import BuildFunction

        if derivation == Derivation.CONSTANT:
            return Granularity.SINGLE_ROW
        elif derivation == Derivation.AGGREGATE:
            if all([x.endswith(ALL_ROWS_CONCEPT) for x in grain.components]):
                return Granularity.SINGLE_ROW
        elif (
            lineage
            and isinstance(lineage, (Function, BuildFunction))
            and lineage.operator in (FunctionType.UNNEST, FunctionType.UNION)
        ):
            return Granularity.MULTI_ROW
        elif lineage and all(
            [x.granularity == Granularity.SINGLE_ROW for x in lineage.concept_arguments]
        ):
            return Granularity.SINGLE_ROW
        return Granularity.MULTI_ROW

    # @property
    # def granularity(self) -> Granularity:
    #     return self.calculate_granularity(self.derivation, self.grain, self.lineage)

    def with_filter(
        self,
        condition: Conditional | Comparison | Parenthetical,
        environment: Environment | None = None,
    ) -> "Concept":
        from trilogy.utility import string_to_hash

        if self.lineage and isinstance(self.lineage, FilterItem):
            if self.lineage.where.conditional == condition:
                return self
        hash = string_to_hash(self.name + str(condition))
        new_lineage = FilterItem(content=self, where=WhereClause(conditional=condition))
        new = Concept(
            name=f"{self.name}_filter_{hash}",
            datatype=self.datatype,
            purpose=self.purpose,
            derivation=self.calculate_derivation(new_lineage, self.purpose),
            granularity=self.granularity,
            metadata=self.metadata,
            lineage=new_lineage,
            keys=(self.keys if self.purpose == Purpose.PROPERTY else None),
            grain=self.grain if self.grain else Grain(components=set()),
            namespace=self.namespace,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
        )
        if environment:
            environment.add_concept(new)
        return new


# class UndefinedConcept(Concept, Mergeable, Namespaced):
#     model_config = ConfigDict(arbitrary_types_allowed=True)
#     name: str
#     line_no: int | None = None
#     datatype: DataType | ListType | StructType | MapType | NumericType = (
#         DataType.UNKNOWN
#     )
#     purpose: Purpose = Purpose.UNKNOWN

#     def with_select_context(
#         self,
#         local_concepts: dict[str, Concept],
#         grain: Grain,
#         environment: Environment,
#     ) -> "Concept":
#         if self.address in local_concepts:
#             rval = local_concepts[self.address]
#             rval = rval.with_select_context(local_concepts, grain, environment)
#             return rval
#         if environment.concepts.fail_on_missing:
#             environment.concepts.raise_undefined(self.address, line_no=self.line_no)
#         return self


class OrderItem(Mergeable, SelectContext, Namespaced, BaseModel):
    # this needs to be a full concept as it may not exist in environment
    expr: Concept | ConceptRef | UndefinedConcept
    order: Ordering

    def with_namespace(self, namespace: str) -> "OrderItem":
        return OrderItem(expr=self.expr.with_namespace(namespace), order=self.order)

    def with_select_context(
        self, local_concepts: dict[str, Concept], grain: Grain, environment: Environment
    ) -> "OrderItem":
        from trilogy.core.models.build import BuildOrderItem

        return BuildOrderItem(
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
    def output(self):
        return self.expr.output


class WindowItem(
    DataTyped, ConceptArgs, Mergeable, Namespaced, SelectContext, BaseModel
):
    type: WindowType
    content: ConceptRef
    order_by: List["OrderItem"]
    over: List["ConceptRef"] = Field(default_factory=list)
    index: Optional[int] = None

    @field_validator("content", mode="before")
    def enforce_concept_ref(cls, v):
        if isinstance(v, Concept):
            return ConceptRef(address=v.address, datatype=v.datatype)
        return v

    @field_validator("over", mode="before")
    def enforce_concept_ref_over(cls, v):
        final = []
        for item in v:
            if isinstance(item, Concept):
                final.append(ConceptRef(address=item.address, datatype=item.datatype))
            else:
                final.append(item)
        return final

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
        from trilogy.core.models.build import BuildWindowItem

        return BuildWindowItem(
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
    def concept_arguments(self) -> List[ConceptRef]:
        return self.arguments

    @property
    def arguments(self) -> List[ConceptRef]:
        output = [self.content]
        for order in self.order_by:
            output += [order.output]
        for item in self.over:
            output += [item]
        return output

    @property
    def output(self) -> ConceptRef:
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


class CaseWhen(Namespaced, ConceptArgs, Mergeable, SelectContext, BaseModel):
    comparison: Conditional | SubselectComparison | Comparison
    expr: "Expr"

    @field_validator("expr", mode="before")
    def enforce_reference(cls, v):
        if isinstance(v, Concept):
            return v.reference
        return v

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
        from trilogy.core.models.build import BuildCaseWhen

        return BuildCaseWhen(
            comparison=self.comparison.with_select_context(
                local_concepts, grain, environment
            ),
            expr=(
                (self.expr.with_select_context(local_concepts, grain, environment))
                if isinstance(self.expr, SelectContext)
                else self.expr
            ),
        )

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> CaseWhen:
        return CaseWhen(
            comparison=self.comparison.with_merge(source, target, modifiers),
            expr=(
                self.expr.with_merge(source, target, modifiers)
                if isinstance(self.expr, Mergeable)
                else self.expr
            ),
        )


class CaseElse(Namespaced, ConceptArgs, Mergeable, SelectContext, BaseModel):
    expr: "Expr"
    # this ensures that it's easily differentiable from CaseWhen
    discriminant: ComparisonOperator = ComparisonOperator.ELSE

    @field_validator("expr", mode="before")
    def enforce_expr(cls, v):
        if isinstance(v, Concept):
            return ConceptRef(address=v.address, datatype=v.datatype)
        return v

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)

    def with_select_context(
        self,
        local_concepts: dict[str, Concept],
        grain: Grain,
        environment: Environment,
    ):
        from trilogy.core.models.build import BuildCaseElse

        return BuildCaseElse(
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

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> CaseElse:
        return CaseElse(
            discriminant=self.discriminant,
            expr=(
                self.expr.with_merge(source, target, modifiers)
                if isinstance(self.expr, Mergeable)
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


def get_concept_row_arguments(expr) -> List["ConceptRef"]:
    output = []
    if isinstance(expr, ConceptRef):
        output += [expr]

    elif isinstance(expr, ConceptArgs):
        output += expr.row_arguments
    return output


def get_concept_arguments(expr) -> List["ConceptRef"]:
    output = []
    if isinstance(expr, ConceptRef):
        output += [expr]

    elif isinstance(
        expr,
        ConceptArgs,
    ):
        output += expr.concept_arguments
    return output


class Function(DataTyped, ConceptArgs, Mergeable, Namespaced, SelectContext, BaseModel):
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
            ConceptRef,
            AggregateWrapper,
            Function,
            Parenthetical,
            CaseWhen,
            CaseElse,
            WindowItem,
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
            list,
            ListWrapper[Any],
        ]
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if "datatype" in str(self):
            raise SyntaxError(str(self))

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
        from trilogy.core.models.build import BuildFunction

        base = BuildFunction(
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

    @field_validator("arguments", mode="before")
    @classmethod
    def parse_arguments(cls, v, info: ValidationInfo):
        from trilogy.core.models.build import BuildConcept
        from trilogy.parsing.exceptions import ParseError

        values = info.data
        arg_count = len(v)
        final = []
        for x in v:
            if isinstance(x, Concept) and not isinstance(x, BuildConcept):
                final.append(x.reference)
            else:
                final.append(x)
        v = final
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
                isinstance(arg, ConceptRef)
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
    def concept_arguments(self) -> List[ConceptRef]:
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


class AggregateWrapper(Mergeable, ConceptArgs, Namespaced, SelectContext, BaseModel):
    function: Function
    by: List[ConceptRef] = Field(default_factory=list)

    @field_validator("by", mode="before")
    @classmethod
    def enforce_concept_ref(cls, v):
        output = []
        for item in v:
            if isinstance(item, Concept):
                output.append(item.reference)
            else:
                output.append(item)
        return output

    def __str__(self):
        grain_str = [str(c) for c in self.by] if self.by else "abstract"
        return f"{str(self.function)}<{grain_str}>"

    @property
    def datatype(self):
        return self.function.datatype

    @property
    def concept_arguments(self) -> List[ConceptRef]:
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
        from trilogy.core.models.build import BuildAggregateWrapper

        if not self.by:
            by = [environment.concepts[c] for c in grain.components]
        else:
            by = [
                x.with_select_context(local_concepts, grain, environment)
                for x in self.by
            ]
        parent = self.function.with_select_context(local_concepts, grain, environment)
        return BuildAggregateWrapper(function=parent, by=by)


class FilterItem(Namespaced, ConceptArgs, SelectContext, BaseModel):
    content: ConceptRef
    where: "WhereClause"

    @field_validator("content", mode="before")
    def enforce_concept_ref(cls, v):
        if isinstance(v, Concept):
            return ConceptRef(address=v.address, datatype=v.datatype)
        return v

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "FilterItem":
        return FilterItem(
            content=self.content.with_merge(source, target, modifiers),
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
        from trilogy.core.models.build import BuildFilterItem

        return BuildFilterItem(
            content=self.content.with_select_context(
                local_concepts, grain, environment
            ),
            where=self.where.with_select_context(local_concepts, grain, environment),
        )

    @property
    def output_datatype(self):
        return self.content.datatype

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


class RowsetItem(SelectContext, Mergeable, ConceptArgs, Namespaced, BaseModel):
    content: Concept
    rowset: RowsetLineage

    def __repr__(self):
        return f"<Rowset<{self.rowset.name}>: {str(self.content)}>"

    def __str__(self):
        return self.__repr__()

    def with_merge(self, source: Concept, target: Concept, modifiers: List[Modifier]):
        return RowsetItem(
            content=self.content.with_merge(source, target, modifiers),
            rowset=self.rowset,
        )

    def with_namespace(self, namespace: str) -> "RowsetItem":
        return RowsetItem(
            content=self.content.with_namespace(namespace),
            rowset=self.rowset.with_namespace(namespace),
        )

    @property
    def arguments(self) -> List[Concept]:
        output = [self.content]
        return output

    @property
    def output(self) -> Concept:
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

    def with_select_context(self, local_concepts, grain, environment):
        from trilogy.core.models.build import BuildRowsetItem

        return BuildRowsetItem(
            content=self.content.with_select_context(
                local_concepts, self.rowset.select.grain, environment
            ),
            rowset=self.rowset,
        )


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
        from trilogy.core.models.build import BuildOrderBy

        return BuildOrderBy(
            items=[
                x.with_select_context(local_concepts, grain, environment)
                for x in self.items
            ]
        )

    @property
    def concept_arguments(self):
        return [x.expr for x in self.items]


class AlignClause(Namespaced, SelectContext, BaseModel):
    items: List[AlignItem]

    def with_select_context(self, local_concepts, grain, environment):
        from trilogy.core.models.build import BuildAlignClause

        return BuildAlignClause(
            items=[
                x.with_select_context(local_concepts, grain, environment)
                for x in self.items
            ]
        )

    def with_namespace(self, namespace: str) -> "AlignClause":
        return AlignClause(items=[x.with_namespace(namespace) for x in self.items])


class SelectLineage(Mergeable, SelectContext, Namespaced, BaseModel):
    selection: List[Concept]
    hidden_components: set[str]
    local_concepts: dict[str, Concept]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    grain: Grain = Field(default_factory=Grain)
    where_clause: Union["WhereClause", None] = Field(default=None)
    having_clause: Union["HavingClause", None] = Field(default=None)

    def build_for_select(self, environment: Environment):
        from trilogy.core.models.build import BuildConcept, BuildSelectLineage

        materialized: dict[str, BuildConcept] = {}
        local_concepts = {
            k: v.with_select_context(materialized, self.grain, environment)
            for k, v in self.local_concepts.items()
        }
        local_concepts = {**local_concepts, **materialized}
        final: List[Concept] = []
        for original in self.selection:
            new = original
            # we don't know the grain of an aggregate at assignment time
            # so rebuild at this point in the tree
            # TODO: simplify
            if new.address in local_concepts:
                new = new.with_select_context(
                    local_concepts,
                    # the first pass grain will be incorrect
                    self.grain,
                    environment=environment,
                )
                local_concepts[new.address] = new
            else:
                # Sometimes cached values here don't have the latest info
                # but we can't just use environment, as it might not have the right grain.
                new = new.with_select_context(
                    local_concepts,
                    self.grain,
                    environment=environment,
                )
                local_concepts[new.address] = new
            final.append(new)

        return BuildSelectLineage(
            selection=final,
            hidden_components=self.hidden_components,
            order_by=(
                self.order_by.with_select_context(
                    local_concepts=local_concepts,
                    grain=self.grain,
                    environment=environment,
                )
                if self.order_by
                else None
            ),
            limit=self.limit,
            meta=self.meta,
            local_concepts=local_concepts,
            grain=self.grain,
            having_clause=(
                self.having_clause.with_select_context(
                    local_concepts=local_concepts,
                    grain=self.grain,
                    environment=environment,
                )
                if self.having_clause
                else None
            ),
            where_clause=(
                self.where_clause.with_select_context(
                    local_concepts={}, grain=Grain(), environment=environment
                )
                if self.where_clause
                else None
            ),
        )

    @property
    def output_components(self) -> List[Concept]:
        return self.selection

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> SelectLineage:
        return SelectLineage(
            selection=[x.with_merge(source, target, modifiers) for x in self.selection],
            hidden_components=self.hidden_components,
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


class MultiSelectLineage(SelectContext, Mergeable, ConceptArgs, Namespaced, BaseModel):
    selects: List[SelectLineage]
    align: AlignClause
    namespace: str
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    where_clause: Union["WhereClause", None] = Field(default=None)
    having_clause: Union["HavingClause", None] = Field(default=None)
    local_concepts: dict[str, Concept]
    hidden_components: set[str]

    def build_for_select(self, environment: Environment):
        from trilogy.core.models.build import BuildMultiSelectLineage

        local_build_cache = {}
        return BuildMultiSelectLineage(
            # we don't build selects here; they'll be built automatically in query discovery
            selects=[x.build_for_select(environment) for x in self.selects],
            align=self.align.with_select_context(
                local_build_cache, self.grain, environment
            ),
            namespace=self.namespace,
            hidden_components=self.hidden_components,
            order_by=(
                self.order_by.with_select_context(
                    local_build_cache, self.grain, environment
                )
                if self.order_by
                else None
            ),
            limit=self.limit,
            where_clause=(
                self.where_clause.with_select_context(
                    local_build_cache, self.grain, environment
                )
                if self.where_clause
                else None
            ),
            having_clause=(
                self.having_clause.with_select_context(
                    local_build_cache, self.grain, environment
                )
                if self.having_clause
                else None
            ),
            local_concepts={
                x: y.with_select_context(local_build_cache, self.grain, environment)
                for x, y in self.local_concepts.items()
            },
        )

    def with_select_context(self, local_concepts, grain, environment):
        return self.build_for_select(environment)

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
            hidden_components=self.hidden_components,
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
            hidden_components=self.hidden_components,
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

    @property
    def output_components(self) -> list[Concept]:
        output = [self.local_concepts[x] for x in self.derived_concepts]
        for select in self.selects:
            output += select.output_components
        return [x for x in output if x.address not in self.hidden_components]

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
            output += select.output_components
        return unique(output, "address")

    # these are needed to help disambiguate between parents
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
    concepts: Sequence[Concept | ConceptRef]

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


class AlignItem(Namespaced, SelectContext, BaseModel):
    alias: str
    concepts: List[ConceptRef]
    namespace: str = Field(default=DEFAULT_NAMESPACE, validate_default=True)

    @field_validator("concepts", mode="before")
    @classmethod
    def enforce_concept_ref(cls, v):
        output = []
        for item in v:
            if isinstance(item, Concept):
                output.append(item.reference)
            else:
                output.append(item)
        return output

    def with_select_context(self, local_concepts, grain, environment):
        from trilogy.core.models.build import BuildAlignItem

        return BuildAlignItem(
            alias=self.alias,
            concepts=[
                x.with_select_context(local_concepts, grain, environment)
                for x in self.concepts
            ],
            namespace=self.namespace,
        )

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
    contents: List[ConceptRef]


class WindowItemOrder(BaseModel):
    contents: List["OrderItem"]


class Comment(BaseModel):
    text: str


Expr = (
    MagicConstants
    | bool
    | int
    | str
    | float
    | list
    | date
    | datetime
    | TupleWrapper
    | ListWrapper
    | WindowItem
    | FilterItem
    | ConceptRef
    | Comparison
    | Conditional
    | Parenthetical
    | Function
    | AggregateWrapper
)
