from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass
from dataclasses import field as dc_field
from dataclasses import replace as dc_replace
from datetime import date, datetime
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Mapping,
    Optional,
    Self,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from trilogy.constants import DEFAULT_NAMESPACE, MagicConstants
from trilogy.core.constants import ALL_ROWS_CONCEPT
from trilogy.core.enums import (
    NAVIGATION_WINDOW_TYPES,
    NUMBERING_WINDOW_TYPES,
    AggregateGroupingMode,
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
    Granularity,
    InfiniteFunctionArgs,
    JoinType,
    Modifier,
    Ordering,
    Purpose,
    SetOperator,
    WindowOrder,
    WindowType,
)
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    TYPEDEF_TYPES,
    Addressable,
    ArrayType,
    DataType,
    DataTyped,
    EnumType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TraitDataType,
    TupleWrapper,
    ValidatedType,
    arg_to_datatype,
    constant_domain_violation,
    is_compatible_datatype,
)
from trilogy.utility import unique

# TODO: refactor to avoid these
if TYPE_CHECKING:
    from trilogy.core.models.environment import Environment


class Namespaced:
    def with_namespace(self, namespace: str):
        raise NotImplementedError


class ReferenceReplaceable:
    def with_reference_replacement(self, replacements: ReferenceReplacements):
        raise NotImplementedError(type(self))


class ConceptArgs:
    @property
    def concept_arguments(self) -> Sequence["ConceptRef"]:
        raise NotImplementedError

    @property
    def existence_arguments(self) -> Sequence[tuple["ConceptRef", ...]]:
        return []

    @property
    def row_arguments(self) -> Sequence["ConceptRef"]:
        return self.concept_arguments


class HasUUID:
    @property
    def uuid(self) -> str:
        return hashlib.md5(str(self).encode()).hexdigest()


def compute_safe_address(namespace: str, name: str) -> str:
    if namespace == DEFAULT_NAMESPACE:
        return name.replace(".", "_")
    elif namespace:
        return f"{namespace.replace('.', '_')}_{name.replace('.', '_')}"
    return name.replace(".", "_")


@dataclass
class ConceptRef(Addressable, Namespaced, DataTyped, ReferenceReplaceable):
    address: str
    datatype: CONCRETE_TYPES = DataType.UNKNOWN
    metadata: Optional["Metadata"] = None

    @property
    def reference(self):
        return self

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

    def __hash__(self):
        return hash(self.address)

    @property
    def namespace(self):
        return self.address.rsplit(".", 1)[0]

    @property
    def name(self):
        return self.address.rsplit(".", 1)[1]

    @property
    def safe_address(self) -> str:
        # A bare address can't distinguish namespace from a dotted property
        # name (`local.create_time.hour` is namespace `local`, name
        # `create_time.hour`), so split on the FIRST dot for the default
        # namespace to match Concept.safe_address; elsewhere the underscore
        # concatenation is identical regardless of split point.
        head, _, tail = self.address.partition(".")
        if head == DEFAULT_NAMESPACE and tail:
            return compute_safe_address(DEFAULT_NAMESPACE, tail)
        return self.address.replace(".", "_")

    @property
    def output_datatype(self):
        return self.datatype

    def with_namespace(self, namespace: str):
        return ConceptRef(
            address=address_with_namespace(self.address, namespace),
            datatype=self.datatype,
            metadata=self.metadata,
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        # a reference might be to an attribute of a struct that is bound late;
        # if a replacement source is a parent in the access path, replace the
        # reference with an attribute access call
        candidates = (f"{DEFAULT_NAMESPACE}.{self.address}", self.address)
        for source, target in replacements:
            for candidate in candidates:
                if candidate == source:
                    return target
                if not candidate.startswith(f"{source}."):
                    continue
                attribute = self.address.rsplit(".", 1)[1]
                dtype = arg_to_datatype(target)
                if not isinstance(dtype, StructType):
                    continue
                output_type = dtype.field_types.get(attribute, DataType.UNKNOWN)
                return Function(
                    arguments=[target, attribute],
                    operator=FunctionType.ATTR_ACCESS,
                    arg_count=2,
                    output_datatype=output_type,
                    output_purpose=Purpose.PROPERTY,
                )
        return self


@dataclass(eq=False)
class UndefinedConcept(ConceptRef):
    @property
    def reference(self):
        return self

    @property
    def purpose(self):
        return Purpose.UNKNOWN


def address_with_namespace(address: str, namespace: str) -> str:
    existing_ns, sep, existing_name = address.partition(".")
    if not sep:
        existing_name = address
    if existing_name == ALL_ROWS_CONCEPT:
        return address
    if existing_ns == DEFAULT_NAMESPACE:
        return f"{namespace}.{existing_name}"
    return f"{namespace}.{address}"


@dataclass
class Parenthetical(
    DataTyped,
    ConceptArgs,
    ReferenceReplaceable,
    Namespaced,
):
    content: "Expr"

    def __post_init__(self):
        if isinstance(self.content, Concept):
            self.content = self.content.reference

    def __add__(self, other) -> Union["Parenthetical", "Conditional"]:
        if other is None:
            return self
        elif isinstance(other, (Comparison, Conditional, Parenthetical, Between)):
            return Conditional(left=self, right=other, operator=BooleanOperator.AND)
        raise ValueError(f"Cannot add {self.__class__} and {type(other)}")

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"({str(self.content)})"

    def with_namespace(self, namespace: str) -> Parenthetical:
        return Parenthetical(
            content=(
                self.content.with_namespace(namespace)
                if isinstance(self.content, Namespaced)
                else self.content
            )
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return Parenthetical(
            content=(
                self.content.with_reference_replacement(replacements)
                if isinstance(self.content, ReferenceReplaceable)
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
    def row_arguments(self) -> Sequence[ConceptRef]:
        if isinstance(self.content, ConceptArgs):
            return self.content.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["ConceptRef", ...]]:
        if isinstance(self.content, ConceptArgs):
            return self.content.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)


@dataclass
class Conditional(ReferenceReplaceable, ConceptArgs, Namespaced, DataTyped):
    left: Expr
    right: Expr
    operator: BooleanOperator

    def __post_init__(self):
        if isinstance(self.left, Concept):
            self.left = self.left.reference
        if isinstance(self.right, Concept):
            self.right = self.right.reference
        if not isinstance(self.operator, BooleanOperator):
            self.operator = BooleanOperator(str(self.operator))

    def __add__(self, other) -> "Conditional":
        if other is None:
            return self
        elif str(other) == str(self):
            return self
        elif isinstance(other, (Comparison, Conditional, Parenthetical, Between)):
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

    def __hash__(self):
        return hash(repr(self))

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

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return self.__class__(
            left=(
                self.left.with_reference_replacement(replacements)
                if isinstance(self.left, ReferenceReplaceable)
                else self.left
            ),
            right=(
                self.right.with_reference_replacement(replacements)
                if isinstance(self.right, ReferenceReplaceable)
                else self.right
            ),
            operator=self.operator,
        )

    @property
    def concept_arguments(self) -> Sequence[ConceptRef]:
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> Sequence[ConceptRef]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> Sequence[tuple[ConceptRef, ...]]:
        output: list[tuple[ConceptRef, ...]] = []
        if isinstance(self.left, ConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, ConceptArgs):
            output += self.right.existence_arguments
        return output

    @property
    def output_datatype(self):
        # a conditional is always a boolean
        return DataType.BOOL

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


@dataclass
class WhereClause(ReferenceReplaceable, ConceptArgs, Namespaced):
    conditional: Union[
        SubselectComparison, Comparison, Conditional, Parenthetical, Between
    ]

    def __repr__(self):
        return str(self.conditional)

    def __str__(self):
        return self.__repr__()

    @property
    def concept_arguments(self) -> Sequence[ConceptRef]:
        return self.conditional.concept_arguments

    @property
    def row_arguments(self) -> Sequence[ConceptRef]:
        return self.conditional.row_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["ConceptRef", ...]]:
        return self.conditional.existence_arguments

    def with_namespace(self, namespace: str) -> Self:
        return self.__class__(conditional=self.conditional.with_namespace(namespace))

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return self.__class__(
            conditional=self.conditional.with_reference_replacement(replacements)
        )


@dataclass
class HavingClause(WhereClause):
    pass


@dataclass
class Grain(Namespaced):
    components: set[str] = dc_field(default_factory=set)
    where_clause: Optional["WhereClause"] = None
    component_order: list[str] = dc_field(default_factory=list)
    _str: Optional[str] = dc_field(default=None, init=False, repr=False, compare=False)
    _abstract: bool = dc_field(default=False, init=False, repr=False, compare=False)

    def __post_init__(self):
        if isinstance(self.components, (list, tuple)):
            output = set()
            ordered = []
            for vc in self.components:
                if isinstance(vc, Addressable):
                    address = vc._address
                else:
                    address = vc
                output.add(address)
                if address not in ordered:
                    ordered.append(address)
            self.components = output
            if not self.component_order:
                self.component_order = ordered
        elif not self.component_order:
            self.component_order = list(self.components)
        else:
            self.component_order = [
                item for item in self.component_order if item in self.components
            ]
            self.component_order.extend(
                item for item in self.components if item not in self.component_order
            )

    def without_condition(self):
        return Grain(
            components=self.components,
            component_order=self.component_order,
        )

    @classmethod
    def from_concepts(
        cls,
        concepts: Iterable[Concept | ConceptRef | str],
        environment: Environment | None = None,
        where_clause: WhereClause | None = None,
        local_concepts: Mapping[str, Concept] | None = None,
    ) -> Grain:
        from trilogy.parsing.common import concepts_to_grain_concepts_ordered

        ordered = concepts_to_grain_concepts_ordered(
            concepts, environment=environment, local_concepts=local_concepts
        )

        return Grain(
            components=set(ordered),
            component_order=ordered,
            where_clause=where_clause,
        )

    def with_namespace(self, namespace: str) -> "Grain":
        order = [address_with_namespace(c, namespace) for c in self.component_order]
        return Grain(
            components=set(order),
            component_order=order,
            where_clause=(
                self.where_clause.with_namespace(namespace)
                if self.where_clause
                else None
            ),
        )

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
        order = self.component_order + [
            item for item in other.component_order if item not in self.components
        ]
        return Grain(
            components=self.components.union(other.components),
            where_clause=where,
            component_order=order,
        )

    def __sub__(self, other: "Grain") -> "Grain":
        return Grain(
            components=self.components.difference(other.components),
            where_clause=self.where_clause,
            component_order=[
                item for item in self.component_order if item not in other.components
            ],
        )

    def _gen_abstract(self) -> bool:
        return not self.components or all(
            [c.endswith(ALL_ROWS_CONCEPT) for c in self.components]
        )

    @property
    def abstract(self):
        if not self._abstract:
            self._abstract = self._gen_abstract()
        return self._abstract

    def __eq__(self, other: object):
        if isinstance(other, list):
            if all([isinstance(c, Concept) for c in other]):
                return self.components == set([c.address for c in other])
            return False
        if not isinstance(other, Grain):
            return False
        if self.components == other.components:
            return True
        return False

    def __hash__(self):
        return hash(frozenset(self.components))

    def issubset(self, other: "Grain"):
        return self.components.issubset(other.components)

    def union(self, other: "Grain"):
        addresses = self.components.union(other.components)
        order = self.component_order + [
            item for item in other.component_order if item not in self.components
        ]
        return Grain(
            components=addresses,
            where_clause=self.where_clause,
            component_order=order,
        )

    def isdisjoint(self, other: "Grain"):
        return self.components.isdisjoint(other.components)

    def intersection(self, other: "Grain") -> "Grain":
        intersection = self.components.intersection(other.components)
        return Grain(
            components=intersection,
            component_order=[
                item for item in self.component_order if item in intersection
            ],
        )

    def _gen_str(self) -> str:
        if self.abstract:
            base = "Grain<Abstract>"
        else:
            base = "Grain<" + ",".join(sorted(self.components)) + ">"
        if self.where_clause:
            base += f"|{str(self.where_clause)}"
        return base

    def __str__(self):
        if not self._str:
            self._str = self._gen_str()
        return self._str

    def __radd__(self, other) -> "Grain":
        if other == 0:
            return self
        else:
            return self.__add__(other)


def _is_row_tuple(x: Any) -> bool:
    """True for a ROW_TUPLE function — an ordered tuple operand of composite
    (row-wise) membership."""
    return isinstance(x, Function) and x.operator == FunctionType.ROW_TUPLE


def _mirror_operator(operator: ComparisonOperator) -> ComparisonOperator:
    """`250 > x` constrains x as `x < 250` — swap direction for the right operand."""
    if operator == ComparisonOperator.GT:
        return ComparisonOperator.LT
    if operator == ComparisonOperator.LT:
        return ComparisonOperator.GT
    if operator == ComparisonOperator.GTE:
        return ComparisonOperator.LTE
    if operator == ComparisonOperator.LTE:
        return ComparisonOperator.GTE
    return operator


@dataclass
class Comparison(ConceptArgs, ReferenceReplaceable, DataTyped, Namespaced):
    left: Union[
        int,
        str,
        float,
        bool,
        datetime,
        date,
        Function,
        ConceptRef,
        Conditional,
        DataType,
        Comparison,
        FunctionCallWrapper,
        Parenthetical,
        MagicConstants,
        WindowItem,
        AggregateWrapper,
        FilterItem,
    ]
    right: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        ConceptRef,
        Function,
        Conditional,
        DataType,
        Comparison,
        FunctionCallWrapper,
        Parenthetical,
        MagicConstants,
        WindowItem,
        AggregateWrapper,
        ListWrapper,
        TupleWrapper,
        FilterItem,
    ]
    operator: ComparisonOperator

    def __post_init__(self):
        if isinstance(self.left, Concept):
            self.left = self.left.reference
        if isinstance(self.right, Concept):
            self.right = self.right.reference
        self._validate_types()

    def _validate_types(self):
        left_type = arg_to_datatype(self.left)
        right_type = arg_to_datatype(self.right)
        left_name = (
            left_type.name if isinstance(left_type, DataType) else str(left_type)
        )
        right_name = (
            right_type.name if isinstance(right_type, DataType) else str(right_type)
        )
        if self.operator in (ComparisonOperator.IS, ComparisonOperator.IS_NOT):
            if self.right != MagicConstants.NULL and DataType.BOOL != right_type:
                raise SyntaxError(
                    f"Cannot use {self.operator.value} with non-null or boolean value {self.right}"
                )
        elif self.operator in (ComparisonOperator.IN, ComparisonOperator.NOT_IN):
            if _is_row_tuple(self.left) and _is_row_tuple(self.right):
                left_elems = self.left.arguments
                right_elems = self.right.arguments
                if len(left_elems) != len(right_elems):
                    raise SyntaxError(
                        f"Composite membership requires matching arity, got "
                        f"{len(left_elems)} and {len(right_elems)} in {str(self)}"
                    )
                for le, re in zip(left_elems, right_elems):
                    lt, rt = arg_to_datatype(le), arg_to_datatype(re)
                    if not is_compatible_datatype(lt, rt):
                        raise SyntaxError(
                            f"Cannot compare composite-membership elements {le} ({lt}) "
                            f"and {re} ({rt}) of different types in {str(self)}"
                        )
            elif isinstance(self.right, TupleWrapper):
                # value-list membership: each element must be comparable to the
                # left scalar (elements need not be comparable to each other)
                for elem in self.right.val:
                    et = arg_to_datatype(elem)
                    if not is_compatible_datatype(left_type, et):
                        raise SyntaxError(
                            f"Cannot compare {left_name} and value-list element "
                            f"{elem} ({et}) with operator {self.operator} in {str(self)}"
                        )
                    if self.operator == ComparisonOperator.IN:
                        violation = constant_domain_violation(
                            left_type, ComparisonOperator.EQ, elem
                        )
                        if violation:
                            raise SyntaxError(
                                f"Impossible comparison in {str(self)}: {violation}"
                            )
            elif isinstance(right_type, ArrayType) and not is_compatible_datatype(
                left_type, right_type.value_data_type
            ):
                raise SyntaxError(
                    f"Cannot compare {left_type} and {right_type} with operator {self.operator} in {str(self)}"
                )
            elif isinstance(self.right, Concept) and not is_compatible_datatype(
                left_type, right_type
            ):
                raise SyntaxError(
                    f"Cannot compare {left_name} and {right_name} with operator {self.operator} in {str(self)}"
                )
        else:
            if not is_compatible_datatype(left_type, right_type):
                raise SyntaxError(
                    f"Cannot compare {left_name} ({self.left}) and {right_name} ({self.right}) of different types with operator {self.operator.value} in {str(self)}"
                )
            violation = constant_domain_violation(left_type, self.operator, self.right)
            if violation is None:
                violation = constant_domain_violation(
                    right_type, _mirror_operator(self.operator), self.left
                )
            if violation:
                raise SyntaxError(f"Impossible comparison in {str(self)}: {violation}")

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(other, (Comparison, Conditional, Parenthetical, Between)):
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

    def __hash__(self):
        return hash(repr(self))

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        # dc_replace so any subclass fields survive the copy
        return dc_replace(
            self,
            left=(
                self.left.with_reference_replacement(replacements)
                if isinstance(self.left, ReferenceReplaceable)
                else self.left
            ),
            right=(
                self.right.with_reference_replacement(replacements)
                if isinstance(self.right, ReferenceReplaceable)
                else self.right
            ),
        )

    def with_namespace(self, namespace: str):
        return dc_replace(
            self,
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
        )

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        """Return concepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[ConceptRef]:
        output = []
        output += get_concept_row_arguments(self.left)
        output += get_concept_row_arguments(self.right)
        return output

    @property
    def existence_arguments(self) -> List[Tuple[ConceptRef, ...]]:
        """Return concepts directly referenced in where clause"""
        output: List[Tuple[ConceptRef, ...]] = []
        if isinstance(self.left, ConceptArgs):
            output += self.left.existence_arguments
        if isinstance(self.right, ConceptArgs):
            output += self.right.existence_arguments
        return output

    @property
    def output_datatype(self):
        # a conditional is always a boolean
        return DataType.BOOL


@dataclass
class Between(ConceptArgs, ReferenceReplaceable, DataTyped, Namespaced):
    left: Union[
        int,
        str,
        float,
        bool,
        datetime,
        date,
        Function,
        ConceptRef,
        DataType,
        FunctionCallWrapper,
        Parenthetical,
        MagicConstants,
        WindowItem,
        AggregateWrapper,
        FilterItem,
    ]
    low: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        ConceptRef,
        Function,
        DataType,
        FunctionCallWrapper,
        Parenthetical,
        MagicConstants,
        WindowItem,
        AggregateWrapper,
        FilterItem,
    ]
    high: Union[
        int,
        str,
        float,
        bool,
        date,
        datetime,
        ConceptRef,
        Function,
        DataType,
        FunctionCallWrapper,
        Parenthetical,
        MagicConstants,
        WindowItem,
        AggregateWrapper,
        FilterItem,
    ]

    def __post_init__(self):
        if isinstance(self.left, Concept):
            self.left = self.left.reference
        if isinstance(self.low, Concept):
            self.low = self.low.reference
        if isinstance(self.high, Concept):
            self.high = self.high.reference
        self._validate_types()

    def _validate_types(self):
        left_type = arg_to_datatype(self.left)
        for bound, label in ((self.low, "low"), (self.high, "high")):
            bound_type = arg_to_datatype(bound)
            if not is_compatible_datatype(left_type, bound_type):
                left_name = (
                    left_type.name
                    if isinstance(left_type, DataType)
                    else str(left_type)
                )
                bound_name = (
                    bound_type.name
                    if isinstance(bound_type, DataType)
                    else str(bound_type)
                )
                raise SyntaxError(
                    f"Cannot use BETWEEN with incompatible types {left_name} and {bound_name} ({label})"
                )
        # x between low and high == x >= low and x <= high; either arm being
        # provably outside the declared domain makes the whole predicate false
        for bound, op in (
            (self.low, ComparisonOperator.GTE),
            (self.high, ComparisonOperator.LTE),
        ):
            violation = constant_domain_violation(left_type, op, bound)
            if violation:
                raise SyntaxError(f"Impossible comparison in {str(self)}: {violation}")

    def __add__(self, other) -> "Conditional | Between":
        if other is None:
            return self
        if not isinstance(other, (Comparison, Conditional, Parenthetical, Between)):
            raise ValueError(f"Cannot add Between to {type(other)}")
        if other == self:
            return self
        return Conditional(left=self, right=other, operator=BooleanOperator.AND)

    def __repr__(self):
        return f"{self.left} between {self.low} and {self.high}"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not isinstance(other, Between):
            return False
        return (
            self.left == other.left
            and self.low == other.low
            and self.high == other.high
        )

    def __hash__(self):
        return hash(repr(self))

    def with_namespace(self, namespace: str) -> "Between":
        return self.__class__(
            left=(
                self.left.with_namespace(namespace)
                if isinstance(self.left, Namespaced)
                else self.left
            ),
            low=(
                self.low.with_namespace(namespace)
                if isinstance(self.low, Namespaced)
                else self.low
            ),
            high=(
                self.high.with_namespace(namespace)
                if isinstance(self.high, Namespaced)
                else self.high
            ),
        )

    def with_reference_replacement(
        self, replacements: ReferenceReplacements
    ) -> "Between":
        return self.__class__(
            left=(
                self.left.with_reference_replacement(replacements)
                if isinstance(self.left, ReferenceReplaceable)
                else self.left
            ),
            low=(
                self.low.with_reference_replacement(replacements)
                if isinstance(self.low, ReferenceReplaceable)
                else self.low
            ),
            high=(
                self.high.with_reference_replacement(replacements)
                if isinstance(self.high, ReferenceReplaceable)
                else self.high
            ),
        )

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        return (
            get_concept_arguments(self.left)
            + get_concept_arguments(self.low)
            + get_concept_arguments(self.high)
        )

    @property
    def row_arguments(self) -> List[ConceptRef]:
        return (
            get_concept_row_arguments(self.left)
            + get_concept_row_arguments(self.low)
            + get_concept_row_arguments(self.high)
        )

    @property
    def existence_arguments(self) -> List[Tuple[ConceptRef, ...]]:
        output: List[Tuple[ConceptRef, ...]] = []
        for child in (self.left, self.low, self.high):
            if isinstance(child, ConceptArgs):
                output += child.existence_arguments
        return output

    @property
    def output_datatype(self):
        return DataType.BOOL


@dataclass(eq=False)
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

    def __hash__(self):
        return hash(repr(self))

    @property
    def row_arguments(self) -> List[ConceptRef]:
        return get_concept_row_arguments(self.left)

    @property
    def existence_arguments(self) -> list[tuple["ConceptRef", ...]]:
        return [tuple(get_concept_arguments(self.right))]


@dataclass
class Concept(Addressable, DataTyped, ConceptArgs, ReferenceReplaceable, Namespaced):
    name: str
    datatype: CONCRETE_TYPES
    purpose: Purpose
    derivation: Derivation = Derivation.ROOT
    granularity: Granularity = Granularity.MULTI_ROW
    metadata: Metadata = dc_field(
        default_factory=lambda: Metadata(description=None, line_number=None)
    )
    lineage: Optional[
        Union[
            Function,
            WindowItem,
            FilterItem,
            AggregateWrapper,
            RowsetItem,
            SubselectItem,
            MultiSelectLineage,
            Comparison,
            Conditional,
            Between,
            "FunctionCallWrapper",
        ]
    ] = None
    namespace: str = dc_field(default=DEFAULT_NAMESPACE)
    keys: Optional[set[str]] = None
    grain: "Grain" = dc_field(default=None)  # type: ignore
    modifiers: List[Modifier] = dc_field(default_factory=list)
    pseudonyms: set[str] = dc_field(default_factory=set)
    address: str = dc_field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.namespace = self.namespace or DEFAULT_NAMESPACE
        self.address = f"{self.namespace}.{self.name}"
        self.metadata = self.metadata or Metadata()
        if isinstance(self.datatype, str):
            self.datatype = DataType(self.datatype)
        if self.purpose == Purpose.AUTO:
            raise ValueError("Cannot set purpose to AUTO")
        # parse grain. FunctionCallWrapper is a syntactic marker — look
        # through it for grain/pseudonym derivation, which depend on the
        # inlined body's shape.
        effective_lineage = (
            self.lineage.content
            if isinstance(self.lineage, FunctionCallWrapper)
            else self.lineage
        )
        if not self.grain and self.purpose == Purpose.KEY:
            self.grain = Grain(components={f"{self.namespace}.{self.name}"})
        elif not self.grain and self.purpose == Purpose.PROPERTY:
            self.grain = Grain(components=self.keys or set())
        elif (
            effective_lineage
            and isinstance(effective_lineage, AggregateWrapper)
            and effective_lineage.by
        ):
            # `by` may carry arbitrary expressions; flatten expression entries
            # to their dependent concept addresses for grain purposes (matches
            # the window over-list handling).
            grain_addrs: set[str] = set()
            for c in effective_lineage.by:
                if isinstance(c, (Concept, ConceptRef)):
                    grain_addrs.add(c.address)
                else:
                    grain_addrs.update(a.address for a in get_concept_arguments(c))
            self.grain = Grain(components=grain_addrs)
        elif not self.grain:
            self.grain = Grain(components=set())
        elif isinstance(self.grain, Grain):
            pass
        elif isinstance(self.grain, Concept):
            self.grain = Grain(components={self.grain.address})
        else:
            raise SyntaxError(f"Invalid grain {self.grain} for concept {self.name}")
        if (
            isinstance(effective_lineage, Function)
            and effective_lineage.operator == FunctionType.ALIAS
        ):
            self.pseudonyms.update(
                arg.address for arg in effective_lineage.concept_arguments
            )

    def duplicate(self) -> Concept:
        return copy.deepcopy(self)

    def __hash__(self):
        return hash(
            f"{self.name}+{self.datatype}+ {self.purpose} + {str(self.lineage)} + {self.namespace} + {str(self.grain)} + {str(self.keys)}"
        )

    def __repr__(self):
        base = f"{self.address}@{self.grain}"
        return base

    @property
    def is_internal(self) -> bool:
        return self.namespace.startswith("_") or self.name.startswith("_")

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

    @cached_property
    def is_aggregate(self):
        return self.calculate_is_aggregate(self.lineage)

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
    def equivalent_addresses(self) -> set[str]:
        return {self.address, *self.pseudonyms}

    @property
    def output(self) -> "Concept":
        return self

    @property
    def safe_address(self) -> str:
        return compute_safe_address(self.namespace, self.name)

    def with_namespace(self, namespace: str) -> Self:
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
        self, grain: Grain, environment: Environment, pin_bare_aggregates: bool = True
    ) -> Tuple[
        Function
        | WindowItem
        | FilterItem
        | AggregateWrapper
        | RowsetItem
        | SubselectItem
        | MultiSelectLineage
        | Comparison
        | None,
        Grain,
        set[str] | None,
    ]:
        # The wrapper is a syntactic marker; semantic shape comes from its
        # inlined body. The build phase will strip the wrapper via
        # `_build_function_call_wrapper`, so we can unwrap here too.
        new_lineage = cast(
            Optional[
                Union[
                    Function,
                    WindowItem,
                    FilterItem,
                    AggregateWrapper,
                    RowsetItem,
                    SubselectItem,
                    MultiSelectLineage,
                    Comparison,
                ]
            ],
            (
                self.lineage.content
                if isinstance(self.lineage, FunctionCallWrapper)
                else self.lineage
            ),
        )
        final_grain = grain if not self.grain.components else self.grain
        keys = self.keys
        if not new_lineage:
            return new_lineage, final_grain, keys

        if isinstance(new_lineage, RowsetItem):
            # A rowset/union output defines its own row identity and must not
            # inherit the consuming select's grain. When that select aggregates
            # over this very output (e.g. `sum(u.v) by u.k`), the inherited grain
            # would be the aggregate itself — a cyclic grain whose parent-concept
            # walk re-adds the aggregate forever (planner RecursionError, q05).
            # Keep the rowset's own grain (abstract for a union, since its stack
            # has no narrower key than the full row).
            return new_lineage, self.grain, keys
        if grain.components and isinstance(new_lineage, Function) and self.is_aggregate:
            aggregate_grain_components = cast(
                List[ConceptRef | Concept],
                _aggregate_pinnable_grain_refs(grain, environment),
            )
            new_lineage = AggregateWrapper(
                function=new_lineage,
                by=aggregate_grain_components,
                grain_inherited=True,
            )
            final_grain = grain
            keys = set(grain.components)
        elif isinstance(new_lineage, AggregateWrapper) and not new_lineage.by:
            if pin_bare_aggregates:
                wrapper_grain_components = cast(
                    List[ConceptRef | Concept],
                    _aggregate_pinnable_grain_refs(grain, environment),
                )
                new_lineage = AggregateWrapper(
                    function=new_lineage.function,
                    by=wrapper_grain_components,
                    grouping=new_lineage.grouping,
                    grouping_sets=new_lineage.grouping_sets,
                    grain_inherited=True,
                )
            final_grain = grain
            keys = set(grain.components)
        elif isinstance(new_lineage, NumberingWindowItem) and not new_lineage.arguments:
            window_grain_components = _grain_concept_refs(grain, environment)
            new_lineage = NumberingWindowItem(
                type=new_lineage.type,
                arguments=window_grain_components,
                over=new_lineage.over,
                order_by=new_lineage.order_by,
            )
            final_grain = grain
            keys = set(grain.components)
        elif self.derivation == Derivation.BASIC:
            assert new_lineage
            by_refs, has_by_aggregate = _aggregate_by_grain_refs(
                new_lineage, environment
            )
            if has_by_aggregate:
                # A row-wise combination of aggregates is a metric whose grain is
                # the union of its aggregates' `by` keys — not the keys of the
                # aggregate inputs (see `_aggregate_by_grain_refs`).
                final_grain = Grain.from_concepts(by_refs, environment)
                keys = final_grain.components
            else:
                pkeys: set[str] = set()
                for x_ref in _row_grain_concept_refs(new_lineage):
                    x = environment.concepts[x_ref.address]
                    if isinstance(x, UndefinedConcept):
                        continue
                    _, _, parent_keys = x.get_select_grain_and_keys(grain, environment)
                    if parent_keys:
                        pkeys.update(parent_keys)
                # deduplicate
                final_grain = Grain.from_concepts(pkeys, environment)
                keys = final_grain.components
        return new_lineage, final_grain, keys

    def set_select_grain(
        self,
        grain: Grain,
        environment: Environment,
        pin_bare_aggregates: bool = True,
    ) -> Self:
        """Assign a mutable concept the appropriate grain/keys for a select"""
        new_lineage, final_grain, keys = self.get_select_grain_and_keys(
            grain, environment, pin_bare_aggregates=pin_bare_aggregates
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

    @cached_property
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
                    SubselectItem,
                    MultiSelectLineage,
                    Comparison,
                    Conditional,
                    Between,
                    "FunctionCallWrapper",
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
        return list(self.lineage.concept_arguments) if self.lineage else []

    @classmethod
    def calculate_derivation(self, lineage, purpose: Purpose) -> Derivation:
        from trilogy.core.models.build import (
            BuildAggregateWrapper,
            BuildBetween,
            BuildComparison,
            BuildConditional,
            BuildFilterItem,
            BuildFunction,
            BuildMultiSelectLineage,
            BuildRowsetItem,
            BuildSubselectItem,
            BuildUnionSelectLineage,
            BuildWindowItem,
        )

        # FunctionCallWrapper is a syntactic marker for `@fn(...)` — derivation
        # is determined by the inlined body, not the wrapper itself.
        if isinstance(lineage, FunctionCallWrapper):
            lineage = lineage.content
        if lineage and isinstance(lineage, (BuildWindowItem, WindowItem)):
            return Derivation.WINDOW
        elif lineage and isinstance(lineage, (BuildFilterItem, FilterItem)):
            return Derivation.FILTER
        elif lineage and isinstance(lineage, (BuildAggregateWrapper, AggregateWrapper)):
            return Derivation.AGGREGATE
        # elif lineage and isinstance(lineage, (BuildParenthetical, Parenthetical)):
        #     return Derivation.PARENTHETICAL
        elif lineage and isinstance(lineage, (BuildRowsetItem, RowsetItem)):
            return Derivation.ROWSET
        elif lineage and isinstance(lineage, (BuildSubselectItem, SubselectItem)):
            return Derivation.SUBSELECT
        # A boolean predicate (`a and b`, `between`) is a row-level basic
        # derivation, just like a single comparison — its parents are sourced
        # and the predicate is computed inline. Without this a Conditional/
        # Between lineage falls through to ROOT and the planner can't source it.
        elif lineage and isinstance(
            lineage,
            (
                BuildComparison,
                Comparison,
                BuildConditional,
                Conditional,
                BuildBetween,
                Between,
            ),
        ):
            return Derivation.BASIC
        elif lineage and isinstance(
            lineage, (BuildUnionSelectLineage, UnionSelectLineage)
        ):
            return Derivation.TVF_UNION
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
            and lineage.operator in FunctionClass.ONE_TO_MANY.value
        ):
            return Derivation.UNNEST
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator == FunctionType.RECURSE_EDGE
        ):
            return Derivation.RECURSIVE
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator == FunctionType.UNION
        ):
            return Derivation.UNION
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator == FunctionType.GROUP
        ):
            return Derivation.GROUP_TO
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator == FunctionType.ALIAS
        ):
            return Derivation.BASIC
        elif (
            lineage
            and isinstance(lineage, (BuildFunction, Function))
            and lineage.operator in FunctionClass.SINGLE_ROW.value
        ):
            return Derivation.CONSTANT

        elif lineage and isinstance(lineage, BuildFunction):
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

    @classmethod
    def _lineage_has_inline_aggregate(cls, node) -> bool:
        # True if the lineage tree contains an aggregate anywhere (e.g. the inline
        # sums in `sum(x)/greatest(sum(1),1)`). Named concepts are NOT recursed
        # into — their granularity is already reflected via concept_arguments.
        from trilogy.core.models.build import BuildAggregateWrapper, BuildFunction

        if isinstance(node, (AggregateWrapper, BuildAggregateWrapper)):
            return True
        if isinstance(node, (Function, BuildFunction)):
            if node.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
                return True
            return any(cls._lineage_has_inline_aggregate(a) for a in node.arguments)
        return False

    @classmethod
    def calculate_granularity(cls, derivation: Derivation, grain: Grain, lineage):
        from trilogy.core.models.build import BuildFilterItem, BuildFunction

        if derivation == Derivation.CONSTANT:
            return Granularity.SINGLE_ROW
        elif derivation == Derivation.AGGREGATE:
            if all([x.endswith(ALL_ROWS_CONCEPT) for x in grain.components]):
                return Granularity.SINGLE_ROW
        elif derivation == Derivation.FILTER and isinstance(lineage, BuildFilterItem):
            # Filtering rows never changes single-row-ness; inherit the filtered
            # content's granularity, ignoring the (multi-row) where-condition args.
            content_args = lineage.content_concept_arguments
            if content_args and all(
                x.granularity == Granularity.SINGLE_ROW for x in content_args
            ):
                return Granularity.SINGLE_ROW
        elif (
            lineage
            and isinstance(lineage, (Function, BuildFunction))
            and lineage.operator
            in (FunctionType.UNNEST, FunctionType.UNION, FunctionType.DATE_SPINE)
        ):
            return Granularity.MULTI_ROW
        elif lineage and all(
            [x.granularity == Granularity.SINGLE_ROW for x in lineage.concept_arguments]
        ):
            return Granularity.SINGLE_ROW
        elif grain.abstract and cls._lineage_has_inline_aggregate(lineage):
            # A grand-total grain reached BY AGGREGATION is single-row — covers
            # BASIC-over-aggregate scalars (e.g. sum(x)/greatest(sum(1),1)) whose
            # flattened concept_arguments look multi-row. Gate on an inline
            # aggregate so an abstract grain produced by a multi-row UNION/bag
            # passthrough (e.g. renaming a union output) stays multi-row.
            return Granularity.SINGLE_ROW
        return Granularity.MULTI_ROW

    # @property
    # def granularity(self) -> Granularity:
    #     return self.calculate_granularity(self.derivation, self.grain, self.lineage)

    def with_filter(
        self,
        condition: Conditional | Comparison | Parenthetical | Between,
        environment: Environment | None = None,
    ) -> "Concept":
        from trilogy.utility import string_to_hash

        if self.lineage and isinstance(self.lineage, FilterItem):
            if self.lineage.where.conditional == condition:
                return self
        hash = string_to_hash(self.name + str(condition))
        new_lineage = FilterItem(
            content=self.reference, where=WhereClause(conditional=condition)
        )
        new = Concept(
            name=f"{self.name}_filter_{hash}",
            datatype=self.datatype,
            purpose=self.purpose,
            derivation=Derivation.FILTER,
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


@dataclass(eq=False)
class UndefinedConceptFull(Concept, ReferenceReplaceable, Namespaced):
    line_no: int | None = None
    datatype: CONCRETE_TYPES = DataType.UNKNOWN
    purpose: Purpose = Purpose.UNKNOWN

    @property
    def reference(self) -> UndefinedConcept:
        return UndefinedConcept(address=self.address)


@dataclass
class OrderItem(ReferenceReplaceable, ConceptArgs, Namespaced):
    # this needs to be a full concept as it may not exist in environment
    expr: Expr
    order: Ordering

    def __post_init__(self):
        if isinstance(self.expr, Concept):
            self.expr = self.expr.reference
        if not isinstance(self.order, Ordering):
            self.order = Ordering(self.order)

    def with_namespace(self, namespace: str) -> "OrderItem":
        return OrderItem(
            expr=(
                self.expr.with_namespace(namespace)
                if isinstance(self.expr, Namespaced)
                else self.expr
            ),
            order=self.order,
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return OrderItem(
            expr=(
                self.expr.with_reference_replacement(replacements)
                if isinstance(self.expr, ReferenceReplaceable)
                else self.expr
            ),
            order=self.order,
        )

    @property
    def concept_arguments(self) -> Sequence[ConceptRef]:
        return get_concept_arguments(self.expr)

    @property
    def row_arguments(self) -> Sequence[ConceptRef]:
        if isinstance(self.expr, ConceptArgs):
            return self.expr.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> Sequence[tuple["ConceptRef", ...]]:
        if isinstance(self.expr, ConceptArgs):
            return self.expr.existence_arguments
        return []

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)


@dataclass
class NumberingWindowItem(DataTyped, ConceptArgs, ReferenceReplaceable, Namespaced):
    type: WindowType
    arguments: List["ConceptRef"]
    order_by: List["OrderItem"]
    # `over` accepts both bare references (`ConceptRef`) and arbitrary
    # expressions (`Function`, `AggregateWrapper`, ...). Expressions are
    # materialized into factory-local concepts at build time; we deliberately
    # keep them un-materialized at parse time so the environment isn't mutated.
    over: List[Any] = dc_field(default_factory=list)

    def __post_init__(self):
        assert (
            self.type in NUMBERING_WINDOW_TYPES
        ), f"NumberingWindowItem requires a numbering window type, got {self.type}"
        self.arguments = [_concept_to_ref(x) for x in self.arguments]
        self.over = [
            _concept_to_ref(x) if isinstance(x, (Concept, ConceptRef)) else x
            for x in self.over
        ]

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.type.value}({self.arguments}) over {self.over} order {self.order_by}"

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return NumberingWindowItem(
            type=self.type,
            arguments=[
                x.with_reference_replacement(replacements) for x in self.arguments
            ],
            over=[x.with_reference_replacement(replacements) for x in self.over],
            order_by=[
                x.with_reference_replacement(replacements) for x in self.order_by
            ],
        )

    def with_namespace(self, namespace: str) -> "NumberingWindowItem":
        return NumberingWindowItem(
            type=self.type,
            arguments=[x.with_namespace(namespace) for x in self.arguments],
            over=[x.with_namespace(namespace) for x in self.over],
            order_by=[x.with_namespace(namespace) for x in self.order_by],
        )

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        output: List[ConceptRef] = []
        for arg in self.arguments:
            output += get_concept_arguments(arg)
        for order in self.order_by:
            output += get_concept_arguments(order)
        for item in self.over:
            output += get_concept_arguments(item)
        return output

    @property
    def output_datatype(self):
        return DataType.INTEGER


@dataclass
class NavigationWindowItem(DataTyped, ConceptArgs, ReferenceReplaceable, Namespaced):
    type: WindowType
    content: FuncArgs
    order_by: List["OrderItem"]
    # `over` accepts both bare references (`ConceptRef`) and arbitrary
    # expressions; expressions are materialized at build time, not parse time.
    over: List[Any] = dc_field(default_factory=list)
    offset: Optional[int] = None

    def __post_init__(self):
        assert (
            self.type in NAVIGATION_WINDOW_TYPES
        ), f"NavigationWindowItem requires a navigation window type, got {self.type}"
        if isinstance(self.content, Concept):
            self.content = ConceptRef(
                address=self.content.address, datatype=self.content.datatype
            )
        self.over = [
            _concept_to_ref(x) if isinstance(x, (Concept, ConceptRef)) else x
            for x in self.over
        ]

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.type.value}({self.content}, offset={self.offset}) over {self.over} order {self.order_by}"

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return NavigationWindowItem(
            type=self.type,
            content=(
                self.content.with_reference_replacement(replacements)
                if isinstance(self.content, ReferenceReplaceable)
                else self.content
            ),
            over=[x.with_reference_replacement(replacements) for x in self.over],
            order_by=[
                x.with_reference_replacement(replacements) for x in self.order_by
            ],
            offset=self.offset,
        )

    def with_namespace(self, namespace: str) -> "NavigationWindowItem":
        return NavigationWindowItem(
            type=self.type,
            content=(
                self.content.with_namespace(namespace)
                if isinstance(self.content, Namespaced)
                else self.content
            ),
            over=[x.with_namespace(namespace) for x in self.over],
            order_by=[x.with_namespace(namespace) for x in self.order_by],
            offset=self.offset,
        )

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        output: List[ConceptRef] = []
        output += get_concept_arguments(self.content)
        for order in self.order_by:
            output += get_concept_arguments(order)
        for item in self.over:
            output += get_concept_arguments(item)
        return output

    @property
    def output_datatype(self):
        if self.type in (WindowType.COUNT, WindowType.COUNT_DISTINCT):
            return DataType.INTEGER
        return self.content.output_datatype


# Window function expressions split by SQL semantics:
# - NumberingWindowItem: rank/dense_rank/row_number — `name() over (...)` (no input).
# - NavigationWindowItem: lag/lead/sum/count/avg/max/min — `name(field) over (...)`.
# Use isinstance(x, WindowItem) to check for any window expression.
WindowItem = NumberingWindowItem | NavigationWindowItem


def get_basic_type(
    type: CONCRETE_TYPES | DataTyped,
) -> DataType:
    if isinstance(type, ArrayType):
        return DataType.ARRAY
    if isinstance(type, StructType):
        return DataType.STRUCT
    if isinstance(type, MapType):
        return DataType.MAP
    if isinstance(type, NumericType):
        return DataType.NUMERIC
    if isinstance(type, EnumType):
        return get_basic_type(type.type)
    if isinstance(type, ValidatedType):
        return get_basic_type(type.type)
    if isinstance(type, TraitDataType):
        return get_basic_type(type.type)
    if isinstance(type, DataTyped):
        return get_basic_type(type.output_datatype)  # type: ignore[attr-defined]
    return type


@dataclass
class CaseSimpleWhen(Namespaced, ConceptArgs, DataTyped, ReferenceReplaceable):
    value_expr: "Expr"
    expr: "Expr"

    def __post_init__(self):
        if isinstance(self.expr, Concept):
            self.expr = self.expr.reference

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"WHEN {str(self.value_expr)} {str(self.expr)}"

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.value_expr) + get_concept_arguments(self.expr)

    @property
    def concept_row_arguments(self):
        return get_concept_row_arguments(self.value_expr) + get_concept_row_arguments(
            self.expr
        )

    def with_namespace(self, namespace: str) -> CaseSimpleWhen:
        return CaseSimpleWhen(
            value_expr=(
                self.value_expr.with_namespace(namespace)
                if isinstance(self.value_expr, Namespaced)
                else self.value_expr
            ),
            expr=(
                self.expr.with_namespace(namespace)
                if isinstance(
                    self.expr,
                    Namespaced,
                )
                else self.expr
            ),
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return CaseSimpleWhen(
            value_expr=(
                self.value_expr.with_reference_replacement(replacements)
                if isinstance(self.value_expr, ReferenceReplaceable)
                else self.value_expr
            ),
            expr=(
                self.expr.with_reference_replacement(replacements)
                if isinstance(self.expr, ReferenceReplaceable)
                else self.expr
            ),
        )


@dataclass
class CaseWhen(Namespaced, DataTyped, ConceptArgs, ReferenceReplaceable):
    comparison: Conditional | SubselectComparison | Comparison | Between
    expr: "Expr"

    def __post_init__(self):
        if isinstance(self.expr, Concept):
            self.expr = self.expr.reference

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)

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

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return CaseWhen(
            comparison=self.comparison.with_reference_replacement(replacements),
            expr=(
                self.expr.with_reference_replacement(replacements)
                if isinstance(self.expr, ReferenceReplaceable)
                else self.expr
            ),
        )


@dataclass
class CaseElse(Namespaced, ConceptArgs, DataTyped, ReferenceReplaceable):
    expr: "Expr"
    # this ensures that it's easily differentiable from CaseWhen
    discriminant: ComparisonOperator = dc_field(
        default_factory=lambda: ComparisonOperator.ELSE
    )

    def __post_init__(self):
        if isinstance(self.expr, Concept):
            self.expr = ConceptRef(
                address=self.expr.address, datatype=self.expr.datatype
            )

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"ELSE {str(self.expr)}"

    @property
    def output_datatype(self):
        return arg_to_datatype(self.expr)

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return CaseElse(
            discriminant=self.discriminant,
            expr=(
                self.expr.with_reference_replacement(replacements)
                if isinstance(self.expr, ReferenceReplaceable)
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


def type_to_pretty(dtype: TYPEDEF_TYPES):
    return str(dtype)


def args_to_pretty(input: set[DataType | ArrayType | MapType]) -> str:
    return ", ".join(
        sorted([f"'{type_to_pretty(x)}'" for x in input if x != DataType.UNKNOWN])
    )


def _matches_valid_type(
    datatype: CONCRETE_TYPES,
    valid_types: set[DataType | ArrayType | MapType],
) -> bool:
    for valid_type in valid_types:
        if isinstance(valid_type, ArrayType):
            if isinstance(datatype, ArrayType) and get_basic_type(
                datatype.type
            ) == get_basic_type(valid_type.type):
                return True
        elif get_basic_type(datatype) == get_basic_type(valid_type):
            return True
    return False


@dataclass
class Function(DataTyped, ConceptArgs, ReferenceReplaceable, Namespaced):
    operator: FunctionType
    output_datatype: CONCRETE_TYPES
    output_purpose: Purpose
    arguments: Sequence[FuncArgs]
    arg_count: int = 1
    valid_inputs: Optional[
        Union[
            Set[DataType | ArrayType | MapType],
            List[Set[DataType | ArrayType | MapType]],
        ]
    ] = None

    def __post_init__(self):
        from trilogy.core.models.build import BuildConcept

        final = []
        for x in self.arguments:
            if isinstance(x, Concept) and not isinstance(x, BuildConcept):
                final.append(x.reference)
            else:
                final.append(x)
        self.arguments = final

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any) -> Any:
        from pydantic_core import core_schema

        schema = handler(source_type)

        def _validate(v: "Function") -> "Function":
            v.validate_arguments()
            return v

        return core_schema.no_info_after_validator_function(_validate, schema)

    def validate_arguments(self) -> None:
        """Run full argument type validation - called from parser."""
        from trilogy.core.exceptions import FunctionArgumentException
        from trilogy.parsing.exceptions import ParseError

        arg_count = len(self.arguments)
        target_arg_count = self.arg_count
        operator_name = self.operator.name
        valid_inputs = self.valid_inputs

        if valid_inputs is None:
            return

        if not arg_count <= target_arg_count:
            if target_arg_count != InfiniteFunctionArgs:
                raise ParseError(
                    f"Incorrect argument count to {operator_name} function, expects"
                    f" {target_arg_count}, got {arg_count}"
                )
        if isinstance(valid_inputs, set):
            valid_inputs = [valid_inputs for _ in self.arguments]
        elif not valid_inputs:
            return
        for idx, arg in enumerate(self.arguments):
            if isinstance(arg, ConceptRef):
                if arg.datatype == DataType.UNKNOWN:
                    continue
                if not _matches_valid_type(arg.datatype, valid_inputs[idx]):
                    raise FunctionArgumentException(
                        f"Invalid argument type '{arg.datatype}' passed into {operator_name} function in position {idx+1}"
                        f" from concept: {arg.address}. Valid: {args_to_pretty(valid_inputs[idx])}."
                    )
            if isinstance(arg, Function):
                if arg.output_datatype != DataType.UNKNOWN and not _matches_valid_type(
                    arg.output_datatype, valid_inputs[idx]
                ):
                    raise FunctionArgumentException(
                        f"Invalid argument type {arg.output_datatype}' passed into"
                        f" {operator_name} function from function {arg.operator.name} in position {idx+1}. Valid: {args_to_pretty(valid_inputs[idx])}"
                    )
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
                    break
                elif isinstance(arg, ptype):
                    if isinstance(arg, str) and DataType.DATE_PART in valid_inputs[idx]:
                        if arg not in [x.value for x in DatePart]:
                            pass
                        else:
                            break
                    raise FunctionArgumentException(
                        f'Invalid {dtype} constant passed into {operator_name} "{arg}", expecting one of {valid_inputs[idx]}'
                    )

    def __repr__(self):
        return f'{self.operator.value}({",".join([str(a) for a in self.arguments])})'

    def __str__(self):
        return self.__repr__()

    @property
    def datatype(self):
        return self.output_datatype

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        from trilogy.core.functions import arg_to_datatype, merge_datatypes

        nargs = [
            (
                c.with_reference_replacement(replacements)
                if isinstance(c, ReferenceReplaceable)
                else c
            )
            for c in self.arguments
        ]
        if self.output_datatype == DataType.UNKNOWN:
            new_output = merge_datatypes([arg_to_datatype(x) for x in nargs])

            if self.operator == FunctionType.ATTR_ACCESS:
                if isinstance(new_output, StructType):
                    new_output = new_output.field_types[str(nargs[1])]
        else:
            new_output = self.output_datatype
        # this is not ideal - see hacky logic for datatypes above
        # we need to figure out how to patch properly
        # should use function factory, but does not have environment access
        # probably move all datatype resolution to build?

        return Function(
            operator=self.operator,
            arguments=nargs,
            output_datatype=new_output,
            output_purpose=self.output_purpose,
            valid_inputs=self.valid_inputs,
            arg_count=self.arg_count,
        )

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
    def concept_arguments(self) -> List[ConceptRef]:
        base = []
        for arg in self.arguments:
            base += get_concept_arguments(arg)
        return base


@dataclass
class FunctionCallWrapper(
    DataTyped,
    ConceptArgs,
    ReferenceReplaceable,
    Namespaced,
):
    content: Expr
    name: str
    args: List[Expr]

    def __str__(self):
        return f'@{self.name}({",".join([str(x) for x in self.args])})'

    def with_namespace(self, namespace) -> "FunctionCallWrapper":
        return FunctionCallWrapper(
            content=(
                self.content.with_namespace(namespace)
                if isinstance(self.content, Namespaced)
                else self.content
            ),
            name=self.name,
            args=[
                x.with_namespace(namespace) if isinstance(x, Namespaced) else x
                for x in self.args
            ],
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return FunctionCallWrapper(
            content=(
                self.content.with_reference_replacement(replacements)
                if isinstance(self.content, ReferenceReplaceable)
                else self.content
            ),
            name=self.name,
            args=[
                (
                    x.with_reference_replacement(replacements)
                    if isinstance(x, ReferenceReplaceable)
                    else x
                )
                for x in self.args
            ],
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
    def output_datatype(self):
        return arg_to_datatype(self.content)


def _concept_to_ref(item: ConceptRef | Concept) -> ConceptRef:
    if isinstance(item, Concept):
        return item.reference
    return item


def _by_item_normalize(item):
    """``by`` accepts arbitrary expressions; coerce only Concepts to refs and
    pass everything else through unchanged. The build phase materializes
    non-concept entries into virtual concepts via ``instantiate_concept``."""
    if isinstance(item, Concept):
        return item.reference
    return item


def _by_item_with_namespace(item, namespace: str):
    if isinstance(item, Namespaced):
        return item.with_namespace(namespace)
    return item


def _by_item_with_reference_replacement(item, replacements):
    if isinstance(item, ReferenceReplaceable):
        return item.with_reference_replacement(replacements)
    return item


def _grain_concept_refs(grain: "Grain", environment: Environment) -> list[ConceptRef]:
    out: list[ConceptRef] = []
    for address in grain.component_order:
        concept = environment.concepts.get(address)
        if concept is None:
            # A select-local virtual (an injected grouping() flag) lives only
            # in the select's local scope; it is grain-relevant downstream but
            # is not a pinnable dimension here.
            continue
        out.append(concept.reference)
    return out


def _aggregate_pinnable_grain_refs(
    grain: "Grain", environment: Environment
) -> list[ConceptRef]:
    """Grain components usable as a bare aggregate's pinned ``by`` dims.

    A grouping-set identity (``grouping()``/``grouping_id()``) is an *output*
    of its grouping pass, not a grouping dimension: it enters the select grain
    as row identity (rollup NULL-padding makes the visible dims non-unique),
    but pinning sibling aggregates by it is circular — flag A's ``by`` would
    contain flag B and vice versa (build RecursionError)."""
    out: list[ConceptRef] = []
    for address in grain.component_order:
        concept = environment.concepts.get(address)
        if concept is None:
            continue
        lineage = concept.lineage
        if isinstance(lineage, AggregateWrapper) and lineage.function.operator in (
            FunctionType.GROUPING,
            FunctionType.GROUPING_ID,
        ):
            continue
        out.append(concept.reference)
    return out


def _row_grain_concept_refs(expr) -> list["ConceptRef"]:
    """Concept arguments that determine a row-wise expression's grain.

    Unlike the flat ``.concept_arguments`` property, a navigation window's
    ``order by`` is excluded: lead/lag emit one value per operand row, so the
    order-by is internal navigation, not a grain dimension. Flattening it up
    would let a wrapping row-op descend the order key to ITS key (a window
    ``order by date.week_seq`` drags the result to date.id), finer than the
    window lives at and blocking the round-into-window merge (q2.1). The
    window's own grain (``_navigation_window_to_concept``) still includes its
    order-by; only outer inference excludes it. Unhandled node types fall back
    to the flat property, preserving prior behavior."""
    if isinstance(expr, ConceptRef):
        return [expr]
    if isinstance(expr, FunctionCallWrapper):
        return _row_grain_concept_refs(expr.content)
    if isinstance(expr, Parenthetical):
        return _row_grain_concept_refs(expr.content)
    if isinstance(expr, NavigationWindowItem):
        out = _row_grain_concept_refs(expr.content)
        for item in expr.over:
            out += _row_grain_concept_refs(item)
        return out
    if isinstance(expr, Function):
        out = []
        for arg in expr.arguments:
            out += _row_grain_concept_refs(arg)
        return out
    if isinstance(expr, (Comparison, Conditional)):
        return _row_grain_concept_refs(expr.left) + _row_grain_concept_refs(expr.right)
    if isinstance(expr, Between):
        return (
            _row_grain_concept_refs(expr.left)
            + _row_grain_concept_refs(expr.low)
            + _row_grain_concept_refs(expr.high)
        )
    return list(get_concept_arguments(expr))


def _aggregate_by_grain_refs(expr, environment: Environment) -> tuple[list[str], bool]:
    """Grain-determining addresses for a row-wise expression that combines
    aggregates (e.g. ``sum(a) + sum(b) by x, y``), plus whether any aggregate
    carried an explicit ``by``.

    By-aggregates contribute their ``by`` keys; abstract aggregates contribute
    nothing (they resolve to the enclosing select grain at build time). The
    aggregate *inputs* are never traversed: the aggregate has already collapsed
    them, so descending would surface source-datasource grain keys (often a
    surrogate primary key) and yield a spurious — frequently circular — grain.
    """
    refs: list[str] = []
    has_by = False
    if isinstance(expr, AggregateWrapper):
        if expr.by:
            for b in expr.by:
                refs.extend(a.address for a in get_concept_arguments(b))
            return refs, True
        return refs, False
    if isinstance(expr, ConceptRef):
        concept = environment.concepts.get(expr.address)
        if concept is not None and concept.is_aggregate and concept.grain.components:
            return list(concept.grain.component_order), True
        return refs, False
    if isinstance(expr, FunctionCallWrapper):
        return _aggregate_by_grain_refs(expr.content, environment)
    if isinstance(expr, Parenthetical):
        return _aggregate_by_grain_refs(expr.content, environment)
    if isinstance(expr, (Comparison, Conditional)):
        left_refs, left_by = _aggregate_by_grain_refs(expr.left, environment)
        right_refs, right_by = _aggregate_by_grain_refs(expr.right, environment)
        return left_refs + right_refs, left_by or right_by
    if isinstance(expr, Function):
        for arg in expr.arguments:
            arg_refs, arg_by = _aggregate_by_grain_refs(arg, environment)
            refs.extend(arg_refs)
            has_by = has_by or arg_by
        return refs, has_by
    return refs, False


@dataclass
class AggregateGrouping:
    mode: AggregateGroupingMode = AggregateGroupingMode.STANDARD
    by: List[Any] = dc_field(default_factory=list)
    grouping_sets: List[List[Any]] = dc_field(default_factory=list)

    def __post_init__(self):
        self.by = [_by_item_normalize(item) for item in self.by]
        self.grouping_sets = [
            [_by_item_normalize(item) for item in grouping_set]
            for grouping_set in self.grouping_sets
        ]

    def with_namespace(self, namespace: str) -> "AggregateGrouping":
        return AggregateGrouping(
            mode=self.mode,
            by=[x.with_namespace(namespace) for x in self.by],
            grouping_sets=[
                [x.with_namespace(namespace) for x in grouping_set]
                for grouping_set in self.grouping_sets
            ],
        )


@dataclass
class AggregateWrapper(ReferenceReplaceable, DataTyped, ConceptArgs, Namespaced):
    function: Function
    by: List[Any] = dc_field(default_factory=list)
    grouping: AggregateGroupingMode = AggregateGroupingMode.STANDARD
    grouping_sets: List[List[Any]] = dc_field(default_factory=list)
    # Set when `by` was filled from the enclosing select's grain because the
    # author pinned none (`sum(x)` not `sum(x) by ...`). Diagnostic metadata
    # only — excluded from equality so it never changes computation identity.
    grain_inherited: bool = dc_field(default=False, compare=False)

    def __post_init__(self):
        self.by = [_by_item_normalize(item) for item in self.by]
        self.grouping_sets = [
            [_by_item_normalize(item) for item in grouping_set]
            for grouping_set in self.grouping_sets
        ]

    def __str__(self):
        if self.grouping == AggregateGroupingMode.GROUPING_SETS:
            grain_str = [[str(c) for c in group] for group in self.grouping_sets]
        else:
            grain_str = [str(c) for c in self.by] if self.by else "abstract"
        return f"{str(self.function)}<{grain_str}>"

    @property
    def datatype(self):
        return self.function.datatype

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        out: List[ConceptRef] = list(self.function.concept_arguments)
        for x in self.by:
            out.extend(get_concept_arguments(x))
        return out

    @property
    def output_datatype(self):
        return self.function.output_datatype

    @property
    def output_purpose(self):
        return self.function.output_purpose

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return AggregateWrapper(
            function=self.function.with_reference_replacement(replacements),
            by=[_by_item_with_reference_replacement(c, replacements) for c in self.by],
            grouping=self.grouping,
            grouping_sets=[
                [
                    _by_item_with_reference_replacement(c, replacements)
                    for c in grouping_set
                ]
                for grouping_set in self.grouping_sets
            ],
            grain_inherited=self.grain_inherited,
        )

    def with_namespace(self, namespace: str) -> "AggregateWrapper":
        return AggregateWrapper(
            function=self.function.with_namespace(namespace),
            by=[_by_item_with_namespace(c, namespace) for c in self.by],
            grouping=self.grouping,
            grouping_sets=[
                [_by_item_with_namespace(c, namespace) for c in grouping_set]
                for grouping_set in self.grouping_sets
            ],
            grain_inherited=self.grain_inherited,
        )


@dataclass
class FilterItem(ReferenceReplaceable, DataTyped, Namespaced, ConceptArgs):
    content: FuncArgs
    where: "WhereClause"

    def __post_init__(self):
        if isinstance(self.content, Concept):
            self.content = ConceptRef(
                address=self.content.address, datatype=self.content.datatype
            )

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    def with_namespace(self, namespace: str) -> "FilterItem":
        return FilterItem(
            content=(
                self.content.with_namespace(namespace)
                if isinstance(self.content, Namespaced)
                else self.content
            ),
            where=self.where.with_namespace(namespace),
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return FilterItem(
            content=(
                self.content.with_reference_replacement(replacements)
                if isinstance(self.content, ReferenceReplaceable)
                else self.content
            ),
            where=self.where.with_reference_replacement(replacements),
        )

    @property
    def output_datatype(self):
        return arg_to_datatype(self.content)

    @property
    def concept_arguments(self):
        if isinstance(self.content, ConceptRef):
            return [self.content] + self.where.concept_arguments
        elif isinstance(self.content, ConceptArgs):
            return self.content.concept_arguments + self.where.concept_arguments
        return self.where.concept_arguments


@dataclass
class SubselectItem(ReferenceReplaceable, DataTyped, Namespaced, ConceptArgs):
    content: ConceptRef
    where: Optional["WhereClause"] = None
    order_by: List["OrderItem"] = dc_field(default_factory=list)
    limit: Optional[int] = None
    outer_arguments: List[ConceptRef] = dc_field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.content, Concept):
            self.content = ConceptRef(
                address=self.content.address, datatype=self.content.datatype
            )

    def __repr__(self):
        parts = [f"subselect({self.content}"]
        if self.where:
            parts.append(f" where {self.where}")
        if self.order_by:
            parts.append(f" order by {self.order_by}")
        if self.limit:
            parts.append(f" limit {self.limit}")
        return "".join(parts) + ")"

    def __str__(self):
        return self.__repr__()

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return SubselectItem(
            content=(
                self.content.with_reference_replacement(replacements)
                if isinstance(self.content, ReferenceReplaceable)
                else self.content
            ),
            where=(
                self.where.with_reference_replacement(replacements)
                if self.where
                else None
            ),
            order_by=[
                x.with_reference_replacement(replacements) for x in self.order_by
            ],
            limit=self.limit,
            outer_arguments=[
                (
                    x.with_reference_replacement(replacements)
                    if isinstance(x, ReferenceReplaceable)
                    else x
                )
                for x in self.outer_arguments
            ],
        )

    def with_namespace(self, namespace: str) -> "SubselectItem":
        return SubselectItem(
            content=(
                self.content.with_namespace(namespace)
                if isinstance(self.content, Namespaced)
                else self.content
            ),
            where=(self.where.with_namespace(namespace) if self.where else None),
            order_by=[x.with_namespace(namespace) for x in self.order_by],
            limit=self.limit,
            outer_arguments=[x.with_namespace(namespace) for x in self.outer_arguments],
        )

    @property
    def output_datatype(self):
        return ArrayType(type=self.content.datatype)

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        # When outer_arguments exist, only expose those to the main query.
        # Inner concepts are resolved separately in gen_subselect_node.
        if self.outer_arguments:
            return list(self.outer_arguments)
        args: List[ConceptRef] = [self.content]
        if self.where:
            args += self.where.concept_arguments
        for item in self.order_by:
            args += get_concept_arguments(item)
        return args


@dataclass
class SubqueryItem(ReferenceReplaceable, DataTyped, ConceptArgs, Namespaced):
    """An inline ``(select …)`` subquery.

    Desugared at hydration into an anonymous rowset. A single-output subquery
    IS that rowset concept (``content``) — build lowers it to the bare concept,
    so a grain-less body cross-joins. A multi-output subquery (only valid as the
    RHS of a tuple ``in``/``not in``) carries every projected output in
    ``contents``; build lowers it to a ROW_TUPLE so membership is row-wise, the
    same shape as authored ``(a, b) in (rs.a, rs.b)``. The ``select`` payload is
    authoring-only: the renderer uses it to reproduce the inline ``(select …)``
    form instead of leaking the synthetic ``_subquery_*`` rowset statement.
    """

    content: ConceptRef
    select: Any
    name: str = ""
    contents: List[ConceptRef] = dc_field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.content, Concept):
            self.content = ConceptRef(
                address=self.content.address, datatype=self.content.datatype
            )
        if not self.contents:
            self.contents = [self.content]

    def __repr__(self):
        if self.is_row:
            return f"<Subquery: ({', '.join(str(c) for c in self.contents)})>"
        return f"<Subquery: {str(self.content)}>"

    def __str__(self):
        return self.__repr__()

    @property
    def is_row(self) -> bool:
        return len(self.contents) > 1

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return SubqueryItem(
            content=(
                self.content.with_reference_replacement(replacements)
                if isinstance(self.content, ReferenceReplaceable)
                else self.content
            ),
            select=self.select,
            name=self.name,
            contents=[
                (
                    c.with_reference_replacement(replacements)
                    if isinstance(c, ReferenceReplaceable)
                    else c
                )
                for c in self.contents
            ],
        )

    def with_namespace(self, namespace: str) -> "SubqueryItem":
        return SubqueryItem(
            content=(
                self.content.with_namespace(namespace)
                if isinstance(self.content, Namespaced)
                else self.content
            ),
            select=self.select,
            name=self.name,
            contents=[
                c.with_namespace(namespace) if isinstance(c, Namespaced) else c
                for c in self.contents
            ],
        )

    @property
    def output(self) -> ConceptRef:
        return self.content

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        return list(self.contents)


@dataclass
class RowsetLineage(Namespaced, ReferenceReplaceable):
    name: str
    derived_concepts: List[ConceptRef]
    select: SelectLineage | MultiSelectLineage

    def with_namespace(self, namespace: str):
        return RowsetLineage(
            name=self.name,
            derived_concepts=[
                x.with_namespace(namespace) for x in self.derived_concepts
            ],
            select=self.select.with_namespace(namespace),
        )


@dataclass
class RowsetItem(ReferenceReplaceable, DataTyped, ConceptArgs, Namespaced):
    content: ConceptRef
    rowset: RowsetLineage

    def __repr__(self):
        return f"<Rowset<{self.rowset.name}>: {str(self.content)}>"

    def __str__(self):
        return self.__repr__()

    def with_namespace(self, namespace: str) -> "RowsetItem":
        return RowsetItem(
            content=self.content.with_namespace(namespace),
            rowset=self.rowset.with_namespace(namespace),
        )

    @property
    def output(self) -> ConceptRef:
        return self.content

    @property
    def output_datatype(self):
        return self.content.datatype

    @property
    def concept_arguments(self):
        return [self.content]


@dataclass
class OrderBy(ReferenceReplaceable, Namespaced):
    items: List[OrderItem]

    def with_namespace(self, namespace: str) -> "OrderBy":
        return OrderBy(items=[x.with_namespace(namespace) for x in self.items])

    @property
    def concept_arguments(self):
        base = []
        for x in self.items:
            base += x.concept_arguments
        return base


@dataclass
class AlignClause(Namespaced):
    items: List[AlignItem]

    def with_namespace(self, namespace: str) -> "AlignClause":
        return AlignClause(items=[x.with_namespace(namespace) for x in self.items])


@dataclass
class DeriveItem(Namespaced, DataTyped, ConceptArgs, ReferenceReplaceable):
    expr: Expr
    name: str
    namespace: str

    @property
    def derived_concept(self) -> str:
        return f"{self.namespace}.{self.name}"
        # return ConceptRef(
        #     address=f"{self.namespace}.{self.name}",
        #     datatype=arg_to_datatype(self.expr),
        # )

    def with_namespace(self, namespace):
        return DeriveItem(
            expr=(self.expr.with_namespace(namespace) if self.expr else None),
            name=self.name,
            namespace=namespace,
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return DeriveItem(
            expr=(
                self.expr.with_reference_replacement(replacements)
                if isinstance(self.expr, ReferenceReplaceable)
                else self.expr
            ),
            name=self.name,
            namespace=self.namespace,
        )


@dataclass
class DeriveClause(ReferenceReplaceable, Namespaced):
    items: List[DeriveItem]

    def with_namespace(self, namespace: str) -> "DeriveClause":
        return DeriveClause(
            items=[
                x.with_namespace(namespace) if isinstance(x, Namespaced) else x
                for x in self.items
            ]
        )

    def with_reference_replacement(self, replacements: ReferenceReplacements):
        return DeriveClause(
            items=[
                (
                    x.with_reference_replacement(replacements)
                    if isinstance(x, ReferenceReplaceable)
                    else x
                )
                for x in self.items
            ]
        )


@dataclass
class SelectLineage(ReferenceReplaceable, Namespaced):
    selection: List[ConceptRef]
    hidden_components: set[str]
    local_concepts: dict[str, Concept]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = dc_field(default_factory=lambda: Metadata())
    grain: Grain = dc_field(default_factory=Grain)
    where_clause: Optional[WhereClause] = None
    having_clause: Optional[HavingClause] = None
    # Query-scoped JOINs declared on this select (`subset|union join a = b`).
    # Carried through to discovery so a select built as a sub-node (e.g. a union
    # arm) applies its own joins — the top-level build reads these off the
    # statement, but a nested arm only sees its lineage.
    scoped_joins: List[tuple[str, str, JoinType]] = dc_field(default_factory=list)
    # SELECT-level multi-level grouping (`by rollup (a, b)` etc.). A select
    # property, not a concept property: the build factory applies it to every
    # un-pinned aggregate materialized in this select's projection scope, so no
    # shared authoring object ever carries the spec.
    grouping: Optional[AggregateGrouping] = None

    @property
    def output_components(self) -> List[ConceptRef]:
        return self.selection

    def with_namespace(self, namespace):
        return SelectLineage(
            selection=[x.with_namespace(namespace) for x in self.selection],
            hidden_components=self.hidden_components,
            local_concepts={
                x: y.with_namespace(namespace) for x, y in self.local_concepts.items()
            },
            order_by=self.order_by.with_namespace(namespace) if self.order_by else None,
            limit=self.limit,
            meta=self.meta,
            grain=self.grain.with_namespace(namespace),
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
            scoped_joins=[
                (
                    address_with_namespace(s, namespace),
                    address_with_namespace(t, namespace),
                    jt,
                )
                for s, t, jt in self.scoped_joins
            ],
            grouping=self.grouping.with_namespace(namespace) if self.grouping else None,
        )


@dataclass
class MultiSelectLineage(ReferenceReplaceable, ConceptArgs, Namespaced):
    selects: List[SelectLineage]
    align: AlignClause
    namespace: str
    hidden_components: set[str]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    where_clause: Optional[WhereClause] = None
    having_clause: Optional[HavingClause] = None
    derive: DeriveClause | None = None

    @property
    def grain(self):
        base = Grain()
        for select in self.selects:
            base += select.grain
        return base

    @property
    def output_components(self) -> list[ConceptRef]:
        align_hidden: set[str] = set()
        for item in self.align.items:
            if item.hidden:
                align_hidden.add(item.aligned_concept)
        select_hidden = self.hidden_components - align_hidden
        output = [
            ConceptRef(address=x, datatype=DataType.UNKNOWN)
            for x in self.derived_concepts
        ]
        for select in self.selects:
            output += select.output_components
        return [x for x in output if x.address not in select_hidden]

    def with_namespace(self, namespace: str) -> "MultiSelectLineage":
        return type(self)(
            selects=[c.with_namespace(namespace) for c in self.selects],
            align=self.align.with_namespace(namespace),
            derive=self.derive.with_namespace(namespace) if self.derive else None,
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
        )

    @property
    def derived_concepts(self) -> set[str]:
        output = set()
        for item in self.align.items:
            output.add(item.aligned_concept)
        if self.derive:
            for ditem in self.derive.items:
                output.add(ditem.derived_concept)
        return output

    @property
    def concept_arguments(self):
        output = []
        for select in self.selects:
            output += select.output_components
        return unique(output, "address")


@dataclass
class UnionSelectLineage(MultiSelectLineage):
    """Positional column-stack (SQL UNION ALL / EXCEPT / INTERSECT) of arm
    selects.

    Shares the multiselect arm structure, but `align` items carry positional
    output bindings (output *i* <- each arm's *i*-th column) and the build
    combiner is a `UnionNode`, not the FULL-JOIN of a multiselect. The visible
    output is exactly the bound union columns — the arms' internal (per-arm
    mangled) columns are never exposed. For EXCEPT the arm order is semantic
    (left-fold), so it must be preserved end-to-end.
    """

    operator: SetOperator = SetOperator.UNION_ALL

    def with_namespace(self, namespace: str) -> "UnionSelectLineage":
        base = super().with_namespace(namespace)
        assert isinstance(base, UnionSelectLineage)
        base.operator = self.operator
        return base

    @property
    def output_components(self) -> list[ConceptRef]:
        # Align-item order is the positional output order; arms' own columns are
        # internal and must not surface.
        return [
            ConceptRef(address=item.aligned_concept, datatype=DataType.UNKNOWN)
            for item in self.align.items
        ]


@dataclass
class LooseConceptList:
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


@dataclass
class AlignItem(Namespaced):
    alias: str
    concepts: List[ConceptRef]
    namespace: str = DEFAULT_NAMESPACE
    hidden: bool = False

    def __post_init__(self):
        output = []
        for item in self.concepts:
            if isinstance(item, Concept):
                output.append(item.reference)
            else:
                output.append(item)
        self.concepts = output

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
            hidden=self.hidden,
        )


class CustomFunctionFactory:
    def __init__(
        self,
        function: Expr,
        namespace: str,
        function_arguments: list[ArgBinding],
        name: str,
    ):
        self.namespace = namespace
        self.function = function
        self.function_arguments = function_arguments
        self.name = name

    def with_namespace(self, namespace: str):
        self.namespace = namespace
        self.function = (
            self.function.with_namespace(namespace)
            if isinstance(self.function, Namespaced)
            else self.function
        )
        self.function_arguments = [
            x.with_namespace(namespace) for x in self.function_arguments
        ]
        return self

    def to_dict(self) -> dict:
        from pydantic import TypeAdapter

        expr_adapter: TypeAdapter[Expr] = TypeAdapter(Expr)
        return {
            "name": self.name,
            "namespace": self.namespace,
            "function": expr_adapter.dump_python(self.function, mode="json"),
            "function_arguments": [
                TypeAdapter(ArgBinding).dump_python(a, mode="json")
                for a in self.function_arguments
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CustomFunctionFactory":
        from pydantic import TypeAdapter

        expr_adapter: TypeAdapter[Expr] = TypeAdapter(Expr)
        return cls(
            name=data["name"],
            namespace=data["namespace"],
            function=expr_adapter.validate_python(data["function"]),
            function_arguments=[
                TypeAdapter(ArgBinding).validate_python(a)
                for a in data["function_arguments"]
            ],
        )

    def __call__(self, *creation_args: ArgBinding | Expr):
        from dataclasses import is_dataclass

        from trilogy.core.exceptions import FunctionArgumentException

        nout = (
            copy.deepcopy(self.function)
            if is_dataclass(self.function)
            else self.function
        )
        creation_arg_list: list[ArgBinding | Expr] = list(creation_args)
        if len(creation_args) < len(self.function_arguments):
            for binding in self.function_arguments[len(creation_arg_list) :]:
                if binding.default is None:
                    raise ValueError(f"Missing argument {binding.name}")

                creation_arg_list.append(binding.default)
        for arg_idx, arg in enumerate(self.function_arguments):
            if not arg.datatype or arg.datatype == DataType.UNKNOWN:
                continue
            if arg_idx > len(creation_arg_list):
                continue
            comparison = arg_to_datatype(creation_arg_list[arg_idx])
            if comparison != arg.datatype:
                raise FunctionArgumentException(
                    f"Invalid type passed into custom function @{self.name} in position {arg_idx+1} for argument {arg.name}, expected {arg.datatype}, got {comparison}"
                )
            if isinstance(arg.datatype, TraitDataType):
                if not (
                    isinstance(comparison, TraitDataType)
                    and all(x in comparison.traits for x in arg.datatype.traits)
                ):
                    raise FunctionArgumentException(
                        f"Invalid argument type passed into custom function @{self.name} in position {arg_idx+1} for argument {arg.name}, expected traits {arg.datatype.traits}, got {comparison}"
                    )
            violation = constant_domain_violation(
                arg.datatype, ComparisonOperator.EQ, creation_arg_list[arg_idx]
            )
            if violation:
                raise FunctionArgumentException(
                    f"Invalid argument passed into custom function @{self.name} in "
                    f"position {arg_idx+1} for argument {arg.name}: {violation}"
                )

        if isinstance(nout, ReferenceReplaceable) and creation_arg_list:
            replacements: ReferenceReplacements = [
                (
                    (
                        f"{DEFAULT_NAMESPACE}.{self.function_arguments[idx].name}"
                        if self.namespace == DEFAULT_NAMESPACE
                        else self.function_arguments[idx].name
                    ),
                    x,
                )
                for idx, x in enumerate(creation_arg_list)
            ]
            nout = nout.with_reference_replacement(replacements)
        if isinstance(nout, SubselectItem) and creation_arg_list:
            outer: list[ConceptRef] = []
            for x in creation_arg_list:
                if isinstance(x, ConceptRef):
                    outer.append(x)
                elif isinstance(x, ConceptArgs):
                    outer.extend(x.concept_arguments)
            nout.outer_arguments = outer
        return nout


@dataclass
class Metadata:
    """Metadata container object.
    TODO: support arbitrary tags"""

    description: Optional[str] = None
    line_number: Optional[int] = None
    column: Optional[int] = None
    end_line: Optional[int] = None
    end_column: Optional[int] = None
    concept_source: ConceptSource = dc_field(default=ConceptSource.MANUAL)
    # Hidden concepts stay fully queryable but are omitted from explore/metadata
    # listings (the `--` declaration prefix, mirroring select-output hiding).
    hidden: bool = False


@dataclass
class Window:
    count: int
    window_order: WindowOrder

    def __str__(self):
        return f"Window<{self.window_order}>"


@dataclass
class WindowItemOver:
    contents: List[ConceptRef]


@dataclass
class WindowItemOrder:
    contents: List["OrderItem"]


@dataclass
class Comment:
    text: str


@dataclass
class ArgBinding(Namespaced, DataTyped):
    name: str
    default: Expr | None = None
    datatype: CONCRETE_TYPES = DataType.UNKNOWN

    def with_namespace(self, namespace):
        return ArgBinding(
            name=address_with_namespace(self.name, namespace),
            default=(
                self.default.with_namespace(namespace)
                if isinstance(self.default, Namespaced)
                else self.default
            ),
        )

    @property
    def output_datatype(self):
        if self.default is not None:
            return arg_to_datatype(self.default)
        return self.datatype


@dataclass
class CustomType:
    name: str
    type: CONCRETE_TYPES | list[CONCRETE_TYPES]
    drop_on: list[FunctionType] = dc_field(default_factory=list)
    add_on: list[FunctionType] = dc_field(default_factory=list)

    def with_namespace(self, namespace: str) -> "CustomType":
        return CustomType(
            name=address_with_namespace(self.name, namespace),
            type=self.type,
            drop_on=self.drop_on,
            add_on=self.add_on,
        )


Expr = (
    MagicConstants
    | bool
    | int
    | str
    | float
    | date
    | datetime
    | TupleWrapper
    | ListWrapper
    | MapWrapper
    | WindowItem
    | FilterItem
    | SubselectItem
    | ConceptRef
    | Comparison
    | Conditional
    | Between
    | FunctionCallWrapper
    | Parenthetical
    | Function
    | AggregateWrapper
    | CaseWhen
    | CaseElse
)

# (source-address, replacement-value) pairs applied in one tree traversal
ReferenceReplacements = Sequence[Tuple[str, Expr | ArgBinding]]

FuncArgs = (
    ConceptRef
    | AggregateWrapper
    | Function
    | FunctionCallWrapper
    | Parenthetical
    | CaseSimpleWhen
    | CaseWhen
    | CaseElse
    | WindowItem
    | FilterItem
    | bool
    | int
    | float
    | DatePart
    | str
    | date
    | datetime
    | MapWrapper[Any, Any]
    | TraitDataType
    | DataType
    | ArrayType
    | MapType
    | NumericType
    | EnumType
    | ValidatedType
    | ListWrapper[Any]
    | TupleWrapper[Any]
    | Comparison
    | Conditional
    | Between
    | MagicConstants
    | ArgBinding
    | Ordering
    | SubselectItem
)
