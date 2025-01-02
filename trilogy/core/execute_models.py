from __future__ import annotations

import difflib
import hashlib
import os
from abc import ABC
from collections import UserDict, UserList, defaultdict, UserString
from datetime import date, datetime
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    Generic,
    ItemsView,
    List,
    Never,
    Optional,
    Self,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
    get_args,
)

from lark.tree import Meta
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    computed_field,
    field_validator,
)
from pydantic.functional_validators import PlainValidator
from pydantic_core import core_schema

from trilogy.constants import (
    CONFIG,
    DEFAULT_NAMESPACE,
    ENV_CACHE_NAME,
    MagicConstants,
    logger,
)
from trilogy.core.core_models import (DataType, Metadata,     MapWrapper,
    ListWrapper,
    ListType,
    StructType,
    MapType,
    arg_to_datatype,
    arg_to_purpose,
    NumericType,
    TupleWrapper,
    Namespaced,
    Reference,
    address_with_namespace,
    is_compatible_datatype)
from trilogy.core.constants import (
    ALL_ROWS_CONCEPT,
    CONSTANT_DATASET,
    INTERNAL_NAMESPACE,
    PERSISTED_CONCEPT_PREFIX,
)
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    ConceptSource,
    DatePart,
    FunctionClass,
    FunctionType,
    Granularity,
    InfiniteFunctionArgs,
    IOType,
    JoinType,
    Modifier,
    Ordering,
    Purpose,
    PurposeLineage,
    SelectFiltering,
    ShowCategory,
    SourceType,
    WindowOrder,
    WindowType,
)
from trilogy.core.exceptions import (
    InvalidSyntaxException,
    UndefinedConceptException,
)
from trilogy.utility import unique

LOGGER_PREFIX = "[MODELS]"


def get_version():
    from trilogy import __version__

    return __version__




def get_concept_arguments(expr) -> List["BoundConcept"]:
    output = []
    if isinstance(expr, BoundConcept):
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


class ConceptArgs(ABC):
    @property
    def concept_arguments(self) -> List["BoundConcept"]:
        raise NotImplementedError

    @property
    def existence_arguments(self) -> list[tuple["BoundConcept", ...]]:
        return []

    @property
    def row_arguments(self) -> List["BoundConcept"]:
        return self.concept_arguments


class SelectContext(ABC):
    def with_select_context(
        self,
        local_concepts: dict[str, BoundConcept],
        grain: Grain,
        environment: BoundEnvironment,
    ) -> Any:
        raise NotImplementedError


class ConstantInlineable(ABC):
    def inline_concept(self, concept: BoundConcept):
        raise NotImplementedError


class HasUUID(ABC):
    @property
    def uuid(self) -> str:
        return hashlib.md5(str(self).encode()).hexdigest()


class SelectTypeMixin(BaseModel):
    where_clause: Union[WhereClause, None] = Field(default=None)
    having_clause: Union[HavingClause, None] = Field(default=None)

    @property
    def output_components(self) -> List[BoundConcept]:
        raise NotImplementedError

    @property
    def implicit_where_clause_selections(self) -> List[BoundConcept]:
        if not self.where_clause:
            return []
        filter = set(
            [
                str(x.address)
                for x in self.where_clause.row_arguments
                if not x.derivation == PurposeLineage.CONSTANT
            ]
        )
        query_output = set([str(z.address) for z in self.output_components])
        delta = filter.difference(query_output)
        if delta:
            return [
                x for x in self.where_clause.row_arguments if str(x.address) in delta
            ]
        return []

    @property
    def where_clause_category(self) -> SelectFiltering:
        if not self.where_clause:
            return SelectFiltering.NONE
        elif self.implicit_where_clause_selections:
            return SelectFiltering.IMPLICIT
        return SelectFiltering.EXPLICIT



class BoundConcept(SelectContext, BaseModel):
    name: str
    datatype: DataType | ListType | StructType | MapType | NumericType
    purpose: Purpose
    metadata: Metadata = Field(
        default_factory=lambda: Metadata(description=None, line_number=None),
        validate_default=True,
    )
    lineage: Optional[Union[
            RowsetItem,
            BoundMultiSelectStatement,
            Function,
            WindowItem,
            FilterItem,
            AggregateWrapper
            ]] = None
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    keys: Optional[set[str]] = Field(default_factory = set)
    grain: "Grain" = Field(default=None, validate_default=True)  # type: ignore
    modifiers: List[Modifier] = Field(default_factory=list)  # type: ignore
    pseudonyms: set[str] = Field(default_factory=set)


    @property
    def reference(self):
        from trilogy.core.author_models import ConceptRef
        return ConceptRef(address=self.address, line_no=self.metadata.line_number)

    def duplicate(self) -> BoundConcept:
        return self.model_copy(deep=True)

    def __hash__(self):
        return hash(
            f"{self.address}+{self.datatype}+ {self.purpose} + {str(self.lineage)} + {str(self.grain)} + {str(self.keys)}"
        )

    def __repr__(self):
        base = f"{self.address}@{self.grain}"
        return base

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
            lineage=self.lineage,
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
        elif isinstance(v, BoundConcept):
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
        if not isinstance(other, BoundConcept):
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
    def output(self) -> "BoundConcept":
        return self

    @property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name.replace(".", "_")
        return self.address.replace(".", "_")

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
        self, local_concepts: dict[str, BoundConcept], grain: Grain, environment: BoundEnvironment
    ) -> BoundConcept:
        new_lineage = self.lineage.instantiate(environment) if isinstance(self.lineage, Reference) else self.lineage
        final_grain = self.grain or grain
        keys = self.keys if self.keys else None
        if self.is_aggregate and isinstance(new_lineage, Function) and grain.components:
            new_lineage = AggregateWrapper(function=new_lineage, by=set(environment.concepts[c] for c in grain.components))
            final_grain = grain
            keys = grain.components
        elif (
            self.is_aggregate and isinstance(new_lineage, AggregateWrapper) and not new_lineage.by
        ):
            new_lineage.by = set(environment.concepts[c] for c in grain.components)
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

    def with_grain(self, grain: Optional["Grain"] = None, name:str| None = None) -> Self:
        return self.__class__(
            name=name or self.name,
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
                    if isinstance(item, BoundConcept):
                        components += [x.address for x in item.sources]
            # TODO: set synonyms
            grain = Grain(
                components=set([x for x in components]),
            )  # synonym_set=generate_concept_synonyms(components))
        elif self.purpose == Purpose.METRIC:
            grain = Grain()
        elif self.purpose == Purpose.CONSTANT:
            if self.derivation != PurposeLineage.CONSTANT:
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
                    # MultiSelectStatement,
                ],
                output: List[BoundConcept],
            ):
                for item in expr.arguments:
                    if isinstance(item, BoundConcept):
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
    def concept_arguments(self) -> List[BoundConcept]:
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
        # elif self.lineage and isinstance(self.lineage, MultiSelectStatement):
        #     return PurposeLineage.MULTISELECT
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
            and self.lineage.operator == FunctionType.UNION
        ):
            return PurposeLineage.UNION
        elif (
            self.lineage
            and isinstance(self.lineage, Function)
            and self.lineage.operator in FunctionClass.SINGLE_ROW.value
        ):
            return PurposeLineage.CONSTANT

        elif self.lineage and isinstance(self.lineage, Function):
            if not self.lineage.concept_arguments:
                return PurposeLineage.CONSTANT
            elif all(
                [
                    x.derivation == PurposeLineage.CONSTANT
                    for x in self.lineage.concept_arguments
                ]
            ):
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
        environment: BoundEnvironment | None = None,
    ) -> "BoundConcept":
        from trilogy.utility import string_to_hash

        if self.lineage and isinstance(self.lineage, FilterItem):
            if self.lineage.where.conditional == condition:
                return self
        hash = string_to_hash(self.name + str(condition))
        new = BoundConcept(
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

    
class Grain(Namespaced, BaseModel):
    components: set[str] = Field(default_factory=set)
    where_clause: Optional["WhereClause"] = None

    def with_merge(self, source: BoundConcept, target: BoundConcept, modifiers: List[Modifier]):
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
        concepts: List[BoundConcept],
        environment: BoundEnvironment | None = None,
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
        from trilogy.core.author_models import ConceptRef, Concept
        output = set()
        if isinstance(v, list):
            for vc in v:
                if isinstance(vc, (BoundConcept, Concept, ConceptRef)):
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
            if not all([isinstance(c, BoundConcept) for c in other]):
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


class BoundEnvironmentConceptDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.undefined: dict[str, UndefinedConcept] = {}
        self.fail_on_missing: bool = True

    def duplicate(self) -> "BoundEnvironmentConceptDict":
        new = BoundEnvironmentConceptDict()
        new.update({k: v.duplicate() for k, v in self.items()})
        new.undefined = self.undefined
        new.fail_on_missing = self.fail_on_missing
        return new

    def values(self) -> ValuesView[BoundConcept]:  # type: ignore
        return super().values()

    def get(self, key: str, default: BoundConcept | None = None) -> BoundConcept | None:  # type: ignore
        try:
            return self.__getitem__(key)
        except UndefinedConceptException:
            return default

    def raise_undefined(
        self, key: str, line_no: int | None = None, file: Path | str | None = None
    ) -> Never:

        matches = self._find_similar_concepts(key)
        message = f"Undefined concept: '{key}'."
        if matches:
            message += f" Suggestions: {matches}"

        if line_no:
            if file:
                raise UndefinedConceptException(
                    f"{file}: {line_no}: " + message, matches
                )
            raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
        raise UndefinedConceptException(message, matches)

    def __getitem__(
        self, key: str, line_no: int | None = None, file: Path | None = None
    ) -> BoundConcept | UndefinedConcept:
        try:
            return super(BoundEnvironmentConceptDict, self).__getitem__(key)
        except KeyError:
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key, line_no)
            if not self.fail_on_missing:
                if "." in key:
                    ns, rest = key.rsplit(".", 1)
                else:
                    ns = DEFAULT_NAMESPACE
                    rest = key
                if key in self.undefined:
                    return self.undefined[key]
                undefined = UndefinedConcept(
                    name=rest,
                    line_no=line_no,
                    datatype=DataType.UNKNOWN,
                    purpose=Purpose.UNKNOWN,
                    namespace=ns,
                )
                self.undefined[key] = undefined
                return undefined
        self.raise_undefined(key, line_no, file)

    def _find_similar_concepts(self, concept_name: str):
        def strip_local(input: str):
            if input.startswith(f"{DEFAULT_NAMESPACE}."):
                return input[len(DEFAULT_NAMESPACE) + 1 :]
            return input

        matches = difflib.get_close_matches(
            strip_local(concept_name), [strip_local(x) for x in self.keys()]
        )
        return matches

    def items(self) -> ItemsView[str, BoundConcept]:  # type: ignore
        return super().items()



class Statement(BaseModel):
    pass


class LooseConceptList(BaseModel):
    concepts: List[BoundConcept]

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
        if not isinstance(other, BoundConcept):
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

class Function( Namespaced, SelectContext, BaseModel):
    operator: FunctionType
    arg_count: int = Field(default=1)
    output_datatype: DataType | ListType | StructType | MapType | NumericType
    output_purpose: Purpose
    valid_inputs: Optional[
        Union[
            Set[DataType | ListType | StructType | MapType | NumericType],
            List[Set[DataType | ListType | StructType | MapType | NumericType]],
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
    def purpose(self):
        return self.output_purpose
    
    @property
    def datatype(self):
        return self.output_datatype

    def with_select_context(
        self, local_concepts: dict[str, BoundConcept], grain: Grain, environment: BoundEnvironment
    ) -> Function:
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
                isinstance(arg, BoundConcept)
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


class Window(BaseModel):
    count: int
    window_order: WindowOrder

    def __str__(self):
        return f"Window<{self.window_order}>"


class WindowItemOver(BaseModel):
    contents: List[BoundConcept]




class WindowItemOrder(BaseModel):
    contents: List["OrderItem"]

class WindowItem(Namespaced, BaseModel):
    type: WindowType
    content: BoundConcept
    order_by: List["OrderItem"]
    over: List["BoundConcept"] = Field(default_factory=list)
    index: Optional[int] = None


    @property
    def datatype(self):
        if self.type in (WindowType.RANK, WindowType.ROW_NUMBER):
            return DataType.INTEGER
        return self.content.datatype
    
    @property
    def purpose(self):
        return Purpose.PROPERTY
    
    @property
    def granularity(self):
        return self.content.granularity
        

    def __repr__(self) -> str:
        return f"{self.type}({self.content} {self.index}, {self.over}, {self.order_by})"

    @property
    def concept_arguments(self) -> List[BoundConcept]:
        return self.arguments

    @property
    def arguments(self) -> List[BoundConcept]:
        output = [self.content]
        for order in self.order_by:
            output += [order.expr]
        for item in self.over:
            output += [item]
        return output

    @property
    def output(self) -> BoundConcept:
        return self.content

    @output.setter
    def output(self, value):
        self.content = value

    @property
    def input(self) -> List[BoundConcept]:
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



class FilterItem( BaseModel):
    content: BoundConcept
    where: "WhereClause"

    def __str__(self):
        return f"<Filter: {str(self.content)} where {str(self.where)}>"

    @property
    def arguments(self) -> List[BoundConcept]:
        output = [self.content]
        output += self.where.input
        return output

    @property
    def output(self) -> BoundConcept:

        return self.content

    @output.setter
    def output(self, value):
        self.content = value

    @property
    def input(self) -> List[BoundConcept]:
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



class OrderItem(BaseModel):
    expr: BoundConcept
    order: Ordering


class OrderBy(BaseModel):
    items: List[OrderItem]


class Address(BaseModel):
    location: str
    is_query: bool = False
    quoted: bool = False


class Query(BaseModel):
    text: str


def safe_concept(v: Union[Dict, BoundConcept]) -> BoundConcept:
    if isinstance(v, dict):
        return BoundConcept.model_validate(v)
    return v




def safe_grain(v) -> Grain:
    if isinstance(v, dict):
        return Grain.model_validate(v)
    elif isinstance(v, Grain):
        return v
    elif not v:
        return Grain(components=set())
    else:
        raise ValueError(f"Invalid input type to safe_grain {type(v)}")


class DatasourceMetadata(BaseModel):
    freshness_concept: BoundConcept | None
    partition_fields: List[BoundConcept] = Field(default_factory=list)
    line_no: int | None = None



class RawColumnExpr(BaseModel):
    text: str


class ColumnAssignment(BaseModel):
    alias: str | RawColumnExpr | Function
    concept: BoundConcept
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers

    def with_grain(self, grain:Grain):
        self.concept = self.concept.with_grain(grain)
        return self





class Datasource(HasUUID, Namespaced, BaseModel):
    name: str
    columns: List[ColumnAssignment]
    address: Union[Address, str]
    grain: Grain = Field(
        default_factory=lambda: Grain(components=set()), validate_default=True
    )
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    metadata: DatasourceMetadata = Field(
        default_factory=lambda: DatasourceMetadata(freshness_concept=None)
    )
    where: Optional[WhereClause] = None
    non_partial_for: Optional[WhereClause] = None

    def duplicate(self) -> Datasource:
        return self.model_copy(deep=True)

    @property
    def hidden_concepts(self) -> List[BoundConcept]:
        return []

    def merge_concept(
        self, source: BoundConcept, target: BoundConcept, modifiers: List[Modifier]
    ):
        original = [c for c in self.columns if c.concept.address == source.address]
        early_exit_check = [
            c for c in self.columns if c.concept.address == target.address
        ]
        if early_exit_check:
            return None
        if len(original) != 1:
            raise ValueError(
                f"Expected exactly one column to merge, got {len(original)} for {source.address}, {[x.alias for x in original]}"
            )
        # map to the alias with the modifier, and the original
        self.columns = [
            c.with_merge(source, target, modifiers)
            for c in self.columns
            if c.concept.address != source.address
        ] + original
        self.grain = self.grain.with_merge(source, target, modifiers)
        self.where = (
            self.where.with_merge(source, target, modifiers) if self.where else None
        )

        self.add_column(target, original[0].alias, modifiers)

    @property
    def identifier(self) -> str:
        if not self.namespace or self.namespace == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}.{self.name}"

    @property
    def safe_identifier(self) -> str:
        return self.identifier.replace(".", "_")

    @property
    def condition(self):
        return None

    @property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_concepts)

    @property
    def can_be_inlined(self) -> bool:
        if isinstance(self.address, Address) and self.address.is_query:
            return False
        # for x in self.columns:
        #     if not isinstance(x.alias, str):
        #         return False
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

    @field_validator("grain", mode="before")
    @classmethod
    def grain_enforcement(cls, v: Grain, info: ValidationInfo):
        grain: Grain = safe_grain(v)
        return grain

    def add_column(
        self,
        concept: BoundConcept,
        alias: str | RawColumnExpr | Function,
        modifiers: List[Modifier] | None = None,
    ):
        self.columns.append(
            ColumnAssignment(alias=alias, concept=concept, modifiers=modifiers or [])
        )

    def __add__(self, other):
        if not other == self:
            raise ValueError(
                "Attempted to add two datasources that are not identical, this is not a valid operation"
            )
        return self

    def __repr__(self):
        return f"Datasource<{self.identifier}@<{self.grain}>"

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return self.identifier.__hash__()

    def with_namespace(self, namespace: str):
        new_namespace = (
            namespace + "." + self.namespace
            if self.namespace and self.namespace != DEFAULT_NAMESPACE
            else namespace
        )
        return Datasource(
            name=self.name,
            namespace=new_namespace,
            grain=self.grain.with_namespace(namespace),
            address=self.address,
            columns=[c.with_namespace(namespace) for c in self.columns],
            where=self.where.with_namespace(namespace) if self.where else None,
        )

    @property
    def concepts(self) -> List[BoundConcept]:
        return [c.concept for c in self.columns]

    @property
    def group_required(self):
        return False

    @property
    def full_concepts(self) -> List[BoundConcept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL not in c.modifiers]

    @property
    def nullable_concepts(self) -> List[BoundConcept]:
        return [c.concept for c in self.columns if Modifier.NULLABLE in c.modifiers]

    @property
    def output_concepts(self) -> List[BoundConcept]:
        return self.concepts

    @property
    def partial_concepts(self) -> List[BoundConcept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL in c.modifiers]

    def get_alias(
        self, concept: BoundConcept, use_raw_name: bool = True, force_alias: bool = False
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
    def safe_location(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address


class UnnestJoin(BaseModel):
    concepts: list[BoundConcept]
    parent: Function
    alias: str = "unnest"
    rendering_required: bool = True

    def __hash__(self):
        return self.safe_identifier.__hash__()

    @property
    def safe_identifier(self) -> str:
        return self.alias + "".join([str(s.address) for s in self.concepts])


class InstantiatedUnnestJoin(BaseModel):
    concept_to_unnest: BoundConcept
    alias: str = "unnest"


class ConceptPair(BaseModel):
    left: BoundConcept
    right: BoundConcept
    existing_datasource: Union[Datasource, "QueryDatasource"]
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def is_partial(self):
        return Modifier.PARTIAL in self.modifiers

    @property
    def is_nullable(self):
        return Modifier.NULLABLE in self.modifiers


class CTEConceptPair(ConceptPair):
    cte: CTE


class BaseJoin(BaseModel):
    right_datasource: Union[Datasource, "QueryDatasource"]
    join_type: JoinType
    concepts: Optional[List[BoundConcept]] = None
    left_datasource: Optional[Union[Datasource, "QueryDatasource"]] = None
    concept_pairs: list[ConceptPair] | None = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        if (
            self.left_datasource
            and self.left_datasource.identifier == self.right_datasource.identifier
        ):
            raise SyntaxError(
                f"Cannot join a dataself to itself, joining {self.left_datasource} and"
                f" {self.right_datasource}"
            )
        final_concepts = []

        # if we have a list of concept pairs
        if self.concept_pairs:
            return
        if self.concepts == []:
            return
        assert self.left_datasource and self.right_datasource
        for concept in self.concepts or []:
            include = True
            for ds in [self.left_datasource, self.right_datasource]:
                synonyms = []
                for c in ds.output_concepts:
                    synonyms += list(c.pseudonyms)
                if (
                    concept.address not in [c.address for c in ds.output_concepts]
                    and concept.address not in synonyms
                ):
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
                if all([c.grain.abstract for c in ds.output_concepts]):
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
        return str(self)

    @property
    def input_concepts(self) -> List[BoundConcept]:
        base = []
        if self.concept_pairs:
            for pair in self.concept_pairs:
                base += [pair.left, pair.right]
        elif self.concepts:
            base += self.concepts
        return base

    def __str__(self):
        if self.concept_pairs:
            return (
                f"{self.join_type.value} {self.right_datasource.name} on"
                f" {','.join([str(k.existing_datasource.name) + '.'+ str(k.left)+'='+str(k.right) for k in self.concept_pairs])}"
            )
        return (
            f"{self.join_type.value} {self.right_datasource.name} on"
            f" {','.join([str(k) for k in self.concepts])}"
        )


class QueryDatasource(BaseModel):
    input_concepts: List[BoundConcept]
    output_concepts: List[BoundConcept]
    datasources: List[Union[Datasource, "QueryDatasource"]]
    source_map: Dict[str, Set[Union[Datasource, "QueryDatasource", "UnnestJoin"]]]

    grain: Grain
    joins: List[BaseJoin | UnnestJoin]
    limit: Optional[int] = None
    condition: Optional[Union["Conditional", "Comparison", "Parenthetical"]] = Field(
        default=None
    )
    filter_concepts: List[BoundConcept] = Field(default_factory=list)
    source_type: SourceType = SourceType.SELECT
    partial_concepts: List[BoundConcept] = Field(default_factory=list)
    hidden_concepts: set[str] = Field(default_factory=set)
    nullable_concepts: List[BoundConcept] = Field(default_factory=list)
    join_derived_concepts: List[BoundConcept] = Field(default_factory=list)
    force_group: bool | None = None
    existence_source_map: Dict[str, Set[Union[Datasource, "QueryDatasource"]]] = Field(
        default_factory=dict
    )

    def __repr__(self):
        return f"{self.identifier}@<{self.grain}>"

    @property
    def safe_identifier(self):
        return self.identifier.replace(".", "_")

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
        unique_pairs = set()
        for join in v:
            if not isinstance(join, BaseJoin):
                continue
            pairing = str(join)
            if pairing in unique_pairs:
                raise SyntaxError(f"Duplicate join {str(join)}")
            unique_pairs.add(pairing)
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
    def validate_source_map(cls, v: dict, info: ValidationInfo):
        values = info.data
        for key in ("input_concepts", "output_concepts"):
            if not values.get(key):
                continue
            concept: BoundConcept
            for concept in values[key]:
                if (
                    concept.address not in v
                    and not any(x in v for x in concept.pseudonyms)
                    and CONFIG.validate_missing
                ):
                    raise SyntaxError(
                        f"On query datasource missing source map for {concept.address} on {key}, have {v}"
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

    def __add__(self, other) -> "QueryDatasource":
        # these are syntax errors to avoid being caught by current
        if not isinstance(other, QueryDatasource):
            raise SyntaxError("Can only merge two query datasources")
        if not other.grain == self.grain:
            raise SyntaxError(
                "Can only merge two query datasources with identical grain"
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

        merged_datasources: dict[str, Union[Datasource, "QueryDatasource"]] = {}

        for ds in [*self.datasources, *other.datasources]:
            if ds.safe_identifier in merged_datasources:
                merged_datasources[ds.safe_identifier] = (
                    merged_datasources[ds.safe_identifier] + ds
                )
            else:
                merged_datasources[ds.safe_identifier] = ds

        final_source_map: defaultdict[
            str, Set[Union[Datasource, "QueryDatasource", "UnnestJoin"]]
        ] = defaultdict(set)

        # add our sources
        for key in self.source_map:
            final_source_map[key] = self.source_map[key].union(
                other.source_map.get(key, set())
            )
        # add their sources
        for key in other.source_map:
            if key not in final_source_map:
                final_source_map[key] = other.source_map[key]

        # if a ds was merged (to combine columns), we need to update the source map
        # to use the merged item
        for k, v in final_source_map.items():
            final_source_map[k] = set(
                merged_datasources.get(x.safe_identifier, x) for x in list(v)
            )
        self_hidden: set[str] = self.hidden_concepts or set()
        other_hidden: set[str] = other.hidden_concepts or set()
        # hidden is the minimum overlapping set
        hidden = self_hidden.intersection(other_hidden)
        qds = QueryDatasource(
            input_concepts=unique(
                self.input_concepts + other.input_concepts, "address"
            ),
            output_concepts=unique(
                self.output_concepts + other.output_concepts, "address"
            ),
            source_map=final_source_map,
            datasources=list(merged_datasources.values()),
            grain=self.grain,
            joins=unique(self.joins + other.joins, "unique_id"),
            condition=(
                self.condition + other.condition
                if self.condition and other.condition
                else self.condition or other.condition
            ),
            source_type=self.source_type,
            partial_concepts=unique(
                self.partial_concepts + other.partial_concepts, "address"
            ),
            join_derived_concepts=self.join_derived_concepts,
            force_group=self.force_group,
            hidden_concepts=hidden,
        )

        return qds

    @property
    def identifier(self) -> str:
        filters = abs(hash(str(self.condition))) if self.condition else ""
        grain = "_".join([str(c).replace(".", "_") for c in self.grain.components])
        return (
            "_join_".join([d.identifier for d in self.datasources])
            + (f"_at_{grain}" if grain else "_at_abstract")
            + (f"_filtered_by_{filters}" if filters else "")
        )

    def get_alias(
        self,
        concept: BoundConcept,
        use_raw_name: bool = False,
        force_alias: bool = False,
        source: str | None = None,
    ):
        for x in self.datasources:
            # query datasources should be referenced by their alias, always
            force_alias = isinstance(x, QueryDatasource)
            #
            use_raw_name = isinstance(x, Datasource) and not force_alias
            if source and x.safe_identifier != source:
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
    output_columns: List[BoundConcept]
    source_map: Dict[str, list[str]]
    grain: Grain
    base: bool = False
    group_to_grain: bool = False
    existence_source_map: Dict[str, list[str]] = Field(default_factory=dict)
    parent_ctes: List[Union["CTE", "UnionCTE"]] = Field(default_factory=list)
    joins: List[Union["Join", "InstantiatedUnnestJoin"]] = Field(default_factory=list)
    condition: Optional[Union["Conditional", "Comparison", "Parenthetical"]] = None
    partial_concepts: List[BoundConcept] = Field(default_factory=list)
    nullable_concepts: List[BoundConcept] = Field(default_factory=list)
    join_derived_concepts: List[BoundConcept] = Field(default_factory=list)
    hidden_concepts: set[str] = Field(default_factory=set)
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    base_name_override: Optional[str] = None
    base_alias_override: Optional[str] = None

    @property
    def identifier(self):
        return self.name

    @property
    def safe_identifier(self):
        return self.name

    @computed_field  # type: ignore
    @property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_columns)

    @field_validator("output_columns")
    def validate_output_columns(cls, v):
        return unique(v, "address")

    def inline_constant(self, concept: BoundConcept):
        if not concept.derivation == PurposeLineage.CONSTANT:
            return False
        if not isinstance(concept.lineage, Function):
            return False
        if not concept.lineage.operator == FunctionType.CONSTANT:
            return False
        # remove the constant
        removed: set = set()
        if concept.address in self.source_map:
            removed = removed.union(self.source_map[concept.address])
            del self.source_map[concept.address]

        if self.condition:
            self.condition = self.condition.inline_constant(concept)

        # if we've entirely removed the need to join to someplace to get the concept
        # drop the join as well.
        for removed_cte in removed:
            still_required = any(
                [
                    removed_cte in x
                    for x in self.source_map.values()
                    or self.existence_source_map.values()
                ]
            )
            if not still_required:
                self.joins = [
                    join
                    for join in self.joins
                    if not isinstance(join, Join)
                    or (
                        isinstance(join, Join)
                        and (
                            join.right_cte.name != removed_cte
                            and any(
                                [
                                    x.cte.name != removed_cte
                                    for x in (join.joinkey_pairs or [])
                                ]
                            )
                        )
                    )
                ]
                for join in self.joins:
                    if isinstance(join, UnnestJoin) and concept in join.concepts:
                        join.rendering_required = False

                self.parent_ctes = [
                    x for x in self.parent_ctes if x.name != removed_cte
                ]
                if removed_cte == self.base_name_override:
                    candidates = [x.name for x in self.parent_ctes]
                    self.base_name_override = candidates[0] if candidates else None
                    self.base_alias_override = candidates[0] if candidates else None
        return True

    @property
    def comment(self) -> str:
        base = f"Target: {str(self.grain)}. Group: {self.group_to_grain}"
        base += f" Source: {self.source.source_type}."
        if self.parent_ctes:
            base += f" References: {', '.join([x.name for x in self.parent_ctes])}."
        if self.joins:
            base += f"\n-- Joins: {', '.join([str(x) for x in self.joins])}."
        if self.partial_concepts:
            base += (
                f"\n-- Partials: {', '.join([str(x) for x in self.partial_concepts])}."
            )
        base += f"\n-- Source Map: {self.source_map}."
        base += f"\n-- Output: {', '.join([str(x) for x in self.output_columns])}."
        if self.source.input_concepts:
            base += f"\n-- Inputs: {', '.join([str(x) for x in self.source.input_concepts])}."
        if self.hidden_concepts:
            base += f"\n-- Hidden: {', '.join([str(x) for x in self.hidden_concepts])}."
        if self.nullable_concepts:
            base += (
                f"\n-- Nullable: {', '.join([str(x) for x in self.nullable_concepts])}."
            )

        return base

    def inline_parent_datasource(self, parent: CTE, force_group: bool = False) -> bool:
        qds_being_inlined = parent.source
        ds_being_inlined = qds_being_inlined.datasources[0]
        if not isinstance(ds_being_inlined, Datasource):
            return False
        if any(
            [
                x.safe_identifier == ds_being_inlined.safe_identifier
                for x in self.source.datasources
            ]
        ):
            return False

        self.source.datasources = [
            ds_being_inlined,
            *[
                x
                for x in self.source.datasources
                if x.safe_identifier != qds_being_inlined.safe_identifier
            ],
        ]
        # need to identify this before updating joins
        if self.base_name == parent.name:
            self.base_name_override = ds_being_inlined.safe_location
            self.base_alias_override = ds_being_inlined.safe_identifier

        for join in self.joins:
            if isinstance(join, InstantiatedUnnestJoin):
                continue
            if (
                join.left_cte
                and join.left_cte.safe_identifier == parent.safe_identifier
            ):
                join.inline_cte(parent)
            if join.joinkey_pairs:
                for pair in join.joinkey_pairs:
                    if pair.cte and pair.cte.safe_identifier == parent.safe_identifier:
                        join.inline_cte(parent)
            if join.right_cte.safe_identifier == parent.safe_identifier:
                join.inline_cte(parent)
        for k, v in self.source_map.items():
            if isinstance(v, list):
                self.source_map[k] = [
                    (
                        ds_being_inlined.safe_identifier
                        if x == parent.safe_identifier
                        else x
                    )
                    for x in v
                ]
            elif v == parent.safe_identifier:
                self.source_map[k] = [ds_being_inlined.safe_identifier]

        # zip in any required values for lookups
        for k in ds_being_inlined.output_lcl.addresses:
            if k in self.source_map and self.source_map[k]:
                continue
            self.source_map[k] = [ds_being_inlined.safe_identifier]
        self.parent_ctes = [
            x for x in self.parent_ctes if x.safe_identifier != parent.safe_identifier
        ]
        if force_group:
            self.group_to_grain = True
        return True

    def __add__(self, other: "CTE" | UnionCTE):
        if isinstance(other, UnionCTE):
            raise ValueError("cannot merge CTE and union CTE")
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
        mutually_hidden = set()
        for concept in self.hidden_concepts:
            if concept in other.hidden_concepts:
                mutually_hidden.add(concept)
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
        self.nullable_concepts = unique(
            self.nullable_concepts + other.nullable_concepts, "address"
        )
        self.hidden_concepts = mutually_hidden
        self.existence_source_map = {
            **self.existence_source_map,
            **other.existence_source_map,
        }
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
    def quote_address(self) -> bool:
        if self.is_root_datasource:
            candidate = self.source.datasources[0]
            if isinstance(candidate, Datasource) and isinstance(
                candidate.address, Address
            ):
                return candidate.address.quoted
        return False

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

    def get_concept(self, address: str) -> BoundConcept | None:
        for cte in self.parent_ctes:
            if address in cte.output_columns:
                match = [x for x in cte.output_columns if x.address == address].pop()
                if match:
                    return match

        for array in [self.source.input_concepts, self.source.output_concepts]:
            match_list = [x for x in array if x.address == address]
            if match_list:
                return match_list.pop()
        match_list = [x for x in self.output_columns if x.address == address]
        if match_list:
            return match_list.pop()
        return None

    def get_alias(self, concept: BoundConcept, source: str | None = None) -> str:
        for cte in self.parent_ctes:
            if concept.address in cte.output_columns:
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
    def group_concepts(self) -> List[BoundConcept]:
        def check_is_not_in_group(c: BoundConcept):
            if len(self.source_map.get(c.address, [])) > 0:
                return False
            if c.derivation == PurposeLineage.ROWSET:
                assert isinstance(c.lineage, RowsetItem)
                return check_is_not_in_group(c.lineage.content)
            if c.derivation == PurposeLineage.CONSTANT:
                return True
            if c.purpose == Purpose.METRIC:
                return True

            if c.derivation == PurposeLineage.BASIC and c.lineage:
                if all([check_is_not_in_group(x) for x in c.lineage.concept_arguments]):
                    return True
                if (
                    isinstance(c.lineage, Function)
                    and c.lineage.operator == FunctionType.GROUP
                ):
                    return check_is_not_in_group(c.lineage.concept_arguments[0])
            return False

        return (
            unique(
                [c for c in self.output_columns if not check_is_not_in_group(c)],
                "address",
            )
            if self.group_to_grain
            else []
        )

    @property
    def render_from_clause(self) -> bool:
        if (
            all([c.derivation == PurposeLineage.CONSTANT for c in self.output_columns])
            and not self.parent_ctes
            and not self.group_to_grain
        ):
            return False
        # if we don't need to source any concepts from anywhere
        # render without from
        # most likely to happen from inlining constants
        if not any([v for v in self.source_map.values()]):
            return False
        if (
            len(self.source.datasources) == 1
            and self.source.datasources[0].name == CONSTANT_DATASET
        ):
            return False
        return True

    @property
    def sourced_concepts(self) -> List[BoundConcept]:
        return [c for c in self.output_columns if c.address in self.source_map]


class UnionCTE(BaseModel):
    name: str
    source: QueryDatasource
    parent_ctes: list[CTE | UnionCTE]
    internal_ctes: list[CTE | UnionCTE]
    output_columns: List[BoundConcept]
    grain: Grain
    operator: str = "UNION ALL"
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    hidden_concepts: set[str] = Field(default_factory=set)
    partial_concepts: list[BoundConcept] = Field(default_factory=list)
    existence_source_map: Dict[str, list[str]] = Field(default_factory=dict)

    @computed_field  # type: ignore
    @property
    def output_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.output_columns)

    def get_alias(self, concept: BoundConcept, source: str | None = None) -> str:
        for cte in self.parent_ctes:
            if concept.address in cte.output_columns:
                if source and source != cte.name:
                    continue
                return concept.safe_address
        return "INVALID_ALIAS"

    def get_concept(self, address: str) -> BoundConcept | None:
        for cte in self.internal_ctes:
            if address in cte.output_columns:
                match = [x for x in cte.output_columns if x.address == address].pop()
                return match

        match_list = [x for x in self.output_columns if x.address == address]
        if match_list:
            return match_list.pop()
        return None

    @property
    def source_map(self):
        return {x.address: [] for x in self.output_columns}

    @property
    def condition(self):
        return None

    @condition.setter
    def condition(self, value):
        raise NotImplementedError

    @property
    def safe_identifier(self):
        return self.name

    @property
    def group_to_grain(self) -> bool:
        return False

    def __add__(self, other):
        if not isinstance(other, UnionCTE) or not other.name == self.name:
            raise SyntaxError("Cannot merge union CTEs")
        return self


def merge_ctes(ctes: List[CTE | UnionCTE]) -> List[CTE | UnionCTE]:
    final_ctes_dict: Dict[str, CTE | UnionCTE] = {}
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
    concept: BoundConcept

    def __str__(self):
        return str(self.concept)

class AlignItem(Namespaced, BaseModel):
    alias: str
    concepts: List[BoundConcept]
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)

    @computed_field  # type: ignore
    @cached_property
    def concepts_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.concepts)



class AlignClause(Namespaced, Reference, BaseModel):
    items: List[AlignItem]

class BoundSelectItem(BaseModel):
    content: Union[BoundConcept, BoundConceptTransform]
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def output(self) -> BoundConcept:
        if isinstance(self.content, BoundConceptTransform):
            return self.content.output
        elif isinstance(self.content, WindowItem):
            return self.content.output
        return self.content

    @property
    def input(self) -> List[BoundConcept]:
        return self.content.input

    def with_namespace(self, namespace: str) -> "BoundSelectItem":
        return BoundSelectItem(
            content=self.content.with_namespace(namespace),
            modifiers=self.modifiers,
        )
    
class BoundConceptTransform(BaseModel):
    function: Function | FilterItem | WindowItem | AggregateWrapper
    output: BoundConcept
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def input(self) -> List[BoundConcept]:
        return [v for v in self.function.arguments if isinstance(v, BoundConcept)]

class BoundSelectStatement(HasUUID,  SelectTypeMixin, BaseModel):
    selection: List[BoundSelectItem]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    local_concepts: Annotated[
        BoundEnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=BoundEnvironmentConceptDict)


    def validate_syntax(self, environment: BoundEnvironment):
        return True
        if self.where_clause:
            for x in self.where_clause.concept_arguments:
                if x.address not in environment.concepts:

                    environment.concepts.raise_undefined(
                        x.address, x.metadata.line_number
                    )
        all_in_output = [x.address for x in self.output_components]
        if self.where_clause:
            for concept in self.where_clause.concept_arguments:
                if (
                    concept.lineage
                    and isinstance(concept.lineage, Function)
                    and concept.lineage.operator
                    in FunctionClass.AGGREGATE_FUNCTIONS.value
                ):
                    if concept.address in self.locally_derived:
                        raise SyntaxError(
                            f"Cannot reference an aggregate derived in the select ({concept.address}) in the same statement where clause; move to the HAVING clause instead; Line: {self.meta.line_number}"
                        )

                if (
                    concept.lineage
                    and isinstance(concept.lineage, AggregateWrapper)
                    and concept.lineage.function.operator
                    in FunctionClass.AGGREGATE_FUNCTIONS.value
                ):
                    if concept.address in self.locally_derived:
                        raise SyntaxError(
                            f"Cannot reference an aggregate derived in the select ({concept.address}) in the same statement where clause; move to the HAVING clause instead; Line: {self.meta.line_number}"
                        )
        if self.having_clause:
            self.having_clause.hydrate_missing(self.local_concepts)
            for concept in self.having_clause.concept_arguments:
                if concept.address not in [x.address for x in self.output_components]:
                    raise SyntaxError(
                        f"Cannot reference a column ({concept.address}) that is not in the select projection in the HAVING clause, move to WHERE;  Line: {self.meta.line_number}"
                    )
        if self.order_by:
            for concept in self.order_by.concept_arguments:
                if concept.address not in all_in_output:
                    raise SyntaxError(
                        f"Cannot order by a column {concept.address} that is not in the output projection; {self.meta.line_number}"
                    )

    def __str__(self):
        from trilogy.parsing.render import render_query

        output = ", ".join([str(x) for x in self.selection])
        return f"SelectStatement<{output}> where {self.where_clause} having {self.having_clause} order by {self.order_by} limit {self.limit}"


    @property
    def locally_derived(self) -> set[str]:
        locally_derived: set[str] = set()
        for item in self.selection:
            if isinstance(item.content, BoundConceptTransform):
                locally_derived.add(item.content.output.address)
        return locally_derived

    @property
    def input_components(self) -> List[BoundConcept]:
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
    def output_components(self) -> List[BoundConcept]:
        output = []
        for item in self.selection:
            if isinstance(item, BoundConcept):
                output.append(item)
            else:
                output.append(item.output)
        return output

    @property
    def hidden_components(self) -> set[str]:
        output = set()
        for item in self.selection:
            if isinstance(item, BoundSelectItem) and Modifier.HIDDEN in item.modifiers:
                output.add(item.output.address)
        return output

    @property
    def all_components(self) -> List[BoundConcept]:
        return self.input_components + self.output_components

    def to_datasource(
        self,
        namespace: str,
        name: str,
        address: Address,
        environment: BoundEnvironment,
        grain: Grain | None = None,
    ) -> Datasource:
        raise NotImplementedError
        # if self.where_clause or self.having_clause:
        #     modifiers = [Modifier.PARTIAL]
        # else:
        #     modifiers = []
        # columns = [
        #     # TODO: replace hardcoded replacement here
        #     # if the concept is a locally derived concept, it cannot ever be partial
        #     # but if it's a concept pulled in from upstream and we have a where clause, it should be partial

        #     ColumnAssignment(
        #         alias=(
        #             c.name.replace(".", "_")
        #             if c.namespace == DEFAULT_NAMESPACE
        #             else c.address.replace(".", "_")
        #         ),
        #         concept=environment.concepts[c.address],
        #         modifiers=modifiers if c.address not in self.locally_derived else [],
        #     )
        #     for c in self.output_components
        # ]

        # condition = None
        # if self.where_clause:
        #     condition = self.where_clause.conditional
        # if self.having_clause:
        #     if condition:
        #         condition = self.having_clause.conditional + condition
        #     else:
        #         condition = self.having_clause.conditional

        # new_datasource = Datasource(
        #     name=name,
        #     address=address,
        #     grain=grain or self.grain,
        #     columns=columns,
        #     namespace=namespace,
        #     non_partial_for=WhereClauseRef(conditional=condition) if condition else None,
        # )
        # for column in columns:
        #     column.concept = column.concept.with_grain(new_datasource.grain)
        # return new_datasource


class BoundMultiSelectStatement(HasUUID, SelectTypeMixin, BaseModel):
    selects: List[BoundSelectStatement]
    align: AlignClause
    namespace: str
    derived_concepts: set[str]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())
    local_concepts: Annotated[
        BoundEnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=BoundEnvironmentConceptDict)

    def __repr__(self):
        return "MultiSelect<" + " MERGE ".join([str(s) for s in self.selects]) + ">"

    @property
    def arguments(self) -> List[BoundConcept]:
        output = []
        for select in self.selects:
            output += select.input_components
        return unique(output, "address")

    @property
    def concept_arguments(self) -> List[BoundConcept]:
        output = []
        for select in self.selects:
            output += select.input_components
        if self.where_clause:
            output += self.where_clause.concept_arguments
        return unique(output, "address")

    def get_merge_concept(self, check: BoundConcept):
        for item in self.align.items:
            if check in item.concepts_lcl:
                return item.gen_concept(self)
        return None


    def find_source(self, concept: BoundConcept, cte: CTE | UnionCTE) ->BoundConcept:
        for x in self.align.items:
            if concept.name == x.alias:
                for c in x.concepts:
                    if c.address in cte.output_lcl:
                        return c
        raise SyntaxError(
            f"Could not find upstream map for multiselect {str(concept)} on cte ({cte})"
        )

    @property
    def output_components(self) -> List[BoundConcept]:
        output = self.derived_concepts
        for select in self.selects:
            output += select.output_components
        return unique(output, "address")

    @computed_field  # type: ignore
    @cached_property
    def hidden_components(self) -> set[str]:
        output: set[str] = set()
        for select in self.selects:
            output = output.union(select.hidden_components)
        return output

class Join(BaseModel):
    right_cte: CTE
    jointype: JoinType
    left_cte: CTE | None = None
    joinkey_pairs: List[CTEConceptPair] | None = None
    inlined_ctes: set[str] = Field(default_factory=set)

    def inline_cte(self, cte: CTE):
        self.inlined_ctes.add(cte.name)

    def get_name(self, cte: CTE):
        if cte.identifier in self.inlined_ctes:
            return cte.source.datasources[0].safe_identifier
        return cte.safe_identifier

    @property
    def right_name(self) -> str:
        if self.right_cte.identifier in self.inlined_ctes:
            return self.right_cte.source.datasources[0].safe_identifier
        return self.right_cte.safe_identifier

    @property
    def right_ref(self) -> str:
        if self.right_cte.identifier in self.inlined_ctes:
            return f"{self.right_cte.source.datasources[0].safe_location} as {self.right_cte.source.datasources[0].safe_identifier}"
        return self.right_cte.safe_identifier

    @property
    def unique_id(self) -> str:
        return str(self)

    def __str__(self):
        if self.joinkey_pairs:
            return (
                f"{self.jointype.value} join"
                f" {self.right_name} on"
                f" {','.join([k.cte.name + '.'+str(k.left.address)+'='+str(k.right.address) for k in self.joinkey_pairs])}"
            )
        elif self.left_cte:
            return (
                f"{self.jointype.value} JOIN {self.left_cte.name} and"
                f" {self.right_name} on {','.join([str(k) for k in self.joinkey_pairs])}"
            )
        return f"{self.jointype.value} JOIN  {self.right_name} on {','.join([str(k) for k in self.joinkey_pairs])}"


class UndefinedConcept(BoundConcept,Namespaced):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str
    line_no: int | None = None
    datatype: DataType | ListType | StructType | MapType | NumericType = (
        DataType.UNKNOWN
    )
    purpose: Purpose = Purpose.UNKNOWN

    def with_select_context(
        self,
        local_concepts: dict[str, BoundConcept],
        grain: Grain,
        environment: BoundEnvironment,
    ) -> "BoundConcept":
        if self.address in local_concepts:
            rval = local_concepts[self.address]
            rval = rval.with_select_context(local_concepts, grain, environment)
            return rval
        if environment.concepts.fail_on_missing:
            environment.concepts.raise_undefined(self.address, line_no=self.line_no)
        return self


class EnvironmentDatasourceDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)

    def __getitem__(self, key: str) -> Datasource:
        try:
            return super(EnvironmentDatasourceDict, self).__getitem__(key)
        except KeyError:
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key)
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1])
            raise

    def values(self) -> ValuesView[Datasource]:  # type: ignore
        return super().values()

    def items(self) -> ItemsView[str, Datasource]:  # type: ignore
        return super().items()

    def duplicate(self) -> "EnvironmentDatasourceDict":
        new = EnvironmentDatasourceDict()
        new.update({k: v.duplicate() for k, v in self.items()})
        return new


class ImportStatement(HasUUID, BaseModel):
    alias: str
    path: Path
    # environment: Union["Environment", None] = None
    # TODO: this might result in a lot of duplication
    # environment:"Environment"


class EnvironmentOptions(BaseModel):
    allow_duplicate_declaration: bool = True


def validate_concepts(v) -> BoundEnvironmentConceptDict:
    if isinstance(v, BoundEnvironmentConceptDict):
        return v
    elif isinstance(v, dict):
        return BoundEnvironmentConceptDict(
            **{x: BoundConcept.model_validate(y) for x, y in v.items()}
        )
    raise ValueError


def validate_datasources(v) -> EnvironmentDatasourceDict:
    if isinstance(v, EnvironmentDatasourceDict):
        return v
    elif isinstance(v, dict):
        return EnvironmentDatasourceDict(
            **{x: Datasource.model_validate(y) for x, y in v.items()}
        )
    raise ValueError


class BoundEnvironment(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, strict=False)

    concepts: Annotated[BoundEnvironmentConceptDict, PlainValidator(validate_concepts)] = (
        Field(default_factory=BoundEnvironmentConceptDict)
    )
    datasources: Annotated[
        EnvironmentDatasourceDict, PlainValidator(validate_datasources)
    ] = Field(default_factory=EnvironmentDatasourceDict)
    functions: Dict[str, Function] = Field(default_factory=dict)
    data_types: Dict[str, DataType] = Field(default_factory=dict)
    imports: Dict[str, list[ImportStatement]] = Field(
        default_factory=lambda: defaultdict(list)  # type: ignore
    )
    namespace: str = DEFAULT_NAMESPACE
    working_path: str | Path = Field(default_factory=lambda: os.getcwd())
    environment_config: EnvironmentOptions = Field(default_factory=EnvironmentOptions)
    version: str = Field(default_factory=get_version)
    cte_name_map: Dict[str, str] = Field(default_factory=dict)
    materialized_concepts: set[str] = Field(default_factory=set)
    alias_origin_lookup: Dict[str, BoundConcept] = Field(default_factory=dict)
    # TODO: support freezing environments to avoid mutation
    frozen: bool = False
    env_file_path: Path | None = None

    def freeze(self):
        self.frozen = True

    def thaw(self):
        self.frozen = False

    def duplicate(self):
        return BoundEnvironment.model_construct(
            datasources=self.datasources.duplicate(),
            concepts=self.concepts.duplicate(),
            functions=dict(self.functions),
            data_types=dict(self.data_types),
            imports=dict(self.imports),
            namespace=self.namespace,
            working_path=self.working_path,
            environment_config=self.environment_config,
            version=self.version,
            cte_name_map=dict(self.cte_name_map),
            materialized_concepts=set(self.materialized_concepts),
            alias_origin_lookup={
                k: v.duplicate() for k, v in self.alias_origin_lookup.items()
            },
        )

    def __init__(self, **data):
        super().__init__(**data)
        # concept = Concept(
        #     name="_env_working_path",
        #     namespace=self.namespace,
        #     lineage=Function(
        #         operator=FunctionType.CONSTANT,
        #         arguments=[str(self.working_path)],
        #         output_datatype=DataType.STRING,
        #         output_purpose=Purpose.CONSTANT,
        #         output_grain = Grain(),
        #     ),
        #     datatype=DataType.STRING,
        #     purpose=Purpose.CONSTANT,
        # )
        # self.add_concept(concept)

    # def freeze(self):
    #     self.frozen = True

    # def thaw(self):
    #     self.frozen = False

    @classmethod
    def from_file(cls, path: str | Path) -> "BoundEnvironment":
        if isinstance(path, str):
            path = Path(path)
        with open(path, "r") as f:
            read = f.read()
        return BoundEnvironment(working_path=path.parent, env_file_path=path).parse(read)[0]

    @classmethod
    def from_string(cls, input: str) -> "BoundEnvironment":
        return BoundEnvironment().parse(input)[0]

    def gen_concept_list_caches(self) -> None:
        concrete_addresses = set()
        for datasource in self.datasources.values():
            for concept in datasource.output_concepts:
                concrete_addresses.add(concept.address)
        self.materialized_concepts = set(
            [
                c.address
                for c in self.concepts.values()
                if c.address in concrete_addresses
            ]
            + [
                c.address
                for c in self.alias_origin_lookup.values()
                if c.address in concrete_addresses
            ],
        )

    def validate_concept(self, new_concept: BoundConcept, meta: Meta | None = None):
        lookup = new_concept.address
        existing: BoundConcept = self.concepts.get(lookup)  # type: ignore
        if not existing:
            return

        def handle_persist():
            deriv_lookup = (
                f"{existing.namespace}.{PERSISTED_CONCEPT_PREFIX}_{existing.name}"
            )

            alt_source = self.alias_origin_lookup.get(deriv_lookup)
            if not alt_source:
                return None
            # if the new concept binding has no lineage
            # nothing to cause us to think a persist binding
            # needs to be invalidated
            if not new_concept.lineage:
                return existing
            if str(alt_source.lineage) == str(new_concept.lineage):
                logger.info(
                    f"Persisted concept {existing.address} matched redeclaration, keeping current persistence binding."
                )
                return existing
            logger.warning(
                f"Persisted concept {existing.address} lineage {str(alt_source.lineage)} did not match redeclaration {str(new_concept.lineage)}, overwriting and invalidating persist binding."
            )
            for k, datasource in self.datasources.items():
                if existing.address in datasource.output_concepts:
                    datasource.columns = [
                        x
                        for x in datasource.columns
                        if x.concept.address != existing.address
                    ]
            return None

        if existing and self.environment_config.allow_duplicate_declaration:
            if existing.metadata.concept_source == ConceptSource.PERSIST_STATEMENT:
                return handle_persist()
            return
        elif existing.metadata:
            if existing.metadata.concept_source == ConceptSource.PERSIST_STATEMENT:
                return handle_persist()
            # if the existing concept is auto derived, we can overwrite it
            if existing.metadata.concept_source == ConceptSource.AUTO_DERIVED:
                return None
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

   





class Comparison(
    ConceptArgs,  Namespaced, ConstantInlineable, SelectContext, BaseModel
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
        BoundConcept,
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
        BoundConcept,
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

    def hydrate_missing(self, concepts: BoundEnvironmentConceptDict):
        if isinstance(self.left, UndefinedConcept) and self.left.address in concepts:
            self.left = concepts[self.left.address]
        if isinstance(self.right, UndefinedConcept) and self.right.address in concepts:
            self.right = concepts[self.right.address]
        return self

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
    
    def validate(self, environment: BoundEnvironment):
        if self.operator in (ComparisonOperator.IS, ComparisonOperator.IS_NOT):
            if self.right != MagicConstants.NULL and DataType.BOOL != arg_to_datatype(
                self.right
            ):
                raise SyntaxError(
                    f"Cannot use {self.operator.value} with non-null or boolean value {self.right}"
                )
        elif self.operator in (ComparisonOperator.IN, ComparisonOperator.NOT_IN):
            right = arg_to_datatype(self.right)
            if not isinstance(self.right, BoundConcept) and not isinstance(right, ListType):
                raise SyntaxError(
                    f"Cannot use {self.operator.value} with non-list type {right} in {str(self)}"
                )

            elif isinstance(right, ListType) and not is_compatible_datatype(
                arg_to_datatype(self.left), right.value_data_type
            ):
                raise SyntaxError(
                    f"Cannot compare {arg_to_datatype(self.left)} and {right} with operator {self.operator} in {str(self)}"
                )
            elif isinstance(self.right, BoundConcept) and not is_compatible_datatype(
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

    def inline_constant(self, constant: BoundConcept):
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
        self, local_concepts: dict[str, BoundConcept], grain: Grain, environment: BoundEnvironment
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
    def input(self) -> List[BoundConcept]:
        output: List[BoundConcept] = []
        if isinstance(self.left, (BoundConcept,)):
            output += [self.left]
        if isinstance(
            self.left, (Comparison, SubselectComparison, Conditional, Parenthetical)
        ):
            output += self.left.input
        if isinstance(self.left, FilterItem):
            output += self.left.concept_arguments
        if isinstance(self.left, Function):
            output += self.left.concept_arguments

        if isinstance(self.right, (BoundConcept,)):
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
    def concept_arguments(self) -> List[BoundConcept]:
        """Return concepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[BoundConcept]:
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
    def existence_arguments(self) -> List[Tuple[BoundConcept, ...]]:
        """Return concepts directly referenced in where clause"""
        output: List[Tuple[BoundConcept, ...]] = []
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
    def row_arguments(self) -> List[BoundConcept]:
        return get_concept_arguments(self.left)

    @property
    def existence_arguments(self) -> list[tuple["BoundConcept", ...]]:
        return [tuple(get_concept_arguments(self.right))]



class CaseWhen( BaseModel):
    comparison: Conditional | SubselectComparison | Comparison
    expr: "Expr"

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"WHEN {str(self.comparison)} THEN {str(self.expr)}"

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.comparison) + get_concept_arguments(self.expr)


class CaseElse( BaseModel):
    expr: "Expr"
    # this ensures that it's easily differentiable from CaseWhen
    discriminant: ComparisonOperator = ComparisonOperator.ELSE

    @property
    def concept_arguments(self):
        return get_concept_arguments(self.expr)


class Conditional(
    ConceptArgs, Namespaced, ConstantInlineable, SelectContext, BaseModel
):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        BoundConcept,
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
        BoundConcept,
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

    def inline_constant(self, constant: BoundConcept) -> "Conditional":
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

    @property
    def input(self) -> List[BoundConcept]:
        """Return concepts directly referenced in where clause"""
        output = []

        for x in (self.left, self.right):
            if isinstance(x, BoundConcept):
                output += x.input
            elif isinstance(x, (Comparison, Conditional)):
                output += x.input
            elif isinstance(x, (Function, Parenthetical, FilterItem)):
                output += x.concept_arguments
        return output

    @property
    def concept_arguments(self) -> List[BoundConcept]:
        """Return concepts directly referenced in where clause"""
        output = []
        output += get_concept_arguments(self.left)
        output += get_concept_arguments(self.right)
        return output

    @property
    def row_arguments(self) -> List[BoundConcept]:
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
    def existence_arguments(self) -> list[tuple["BoundConcept", ...]]:
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



class AggregateWrapper(SelectContext, BaseModel):
    function: Function
    by: set[BoundConcept] = Field(default_factory=set)

    def __str__(self):
        grain_str = [str(c) for c in self.by] if self.by else "abstract"
        return f"{str(self.function)}<{grain_str}>"

    @property
    def datatype(self):
        return self.function.output_datatype
    
    @property
    def purpose(self):
        base = self.function.purpose
        if self.by and base == Purpose.METRIC:
            return Purpose.PROPERTY
        return base

    @property
    def concept_arguments(self) -> List[BoundConcept]:
        return self.function.concept_arguments + list(self.by)

    @property
    def output_datatype(self):
        return self.function.output_datatype

    @property
    def output_purpose(self):
        return self.function.output_purpose

    @property
    def arguments(self):
        return self.function.arguments




class WhereClause(ConceptArgs, Namespaced, SelectContext, BaseModel):
    conditional: Union[SubselectComparison, Comparison, Conditional, "Parenthetical"]

    def __repr__(self):
        return str(self.conditional)

    @property
    def input(self) -> List[BoundConcept]:
        return self.conditional.input

    @property
    def concept_arguments(self) -> List[BoundConcept]:
        return self.conditional.concept_arguments

    @property
    def row_arguments(self) -> List[BoundConcept]:
        return self.conditional.row_arguments

    @property
    def existence_arguments(self) -> list[tuple["BoundConcept", ...]]:
        return self.conditional.existence_arguments

    def with_merge(self, source: BoundConcept, target: BoundConcept, modifiers: List[Modifier]):
        return WhereClause(
            conditional=self.conditional.with_merge(source, target, modifiers)
        )

    def with_namespace(self, namespace: str) -> WhereClause:
        return WhereClause(conditional=self.conditional.with_namespace(namespace))

    def with_select_context(
        self, local_concepts: dict[str, BoundConcept], grain: Grain, environment: BoundEnvironment
    ) -> WhereClause:
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


class HavingClause(WhereClause):
    pass

    def hydrate_missing(self, concepts: BoundEnvironmentConceptDict):
        self.conditional.hydrate_missing(concepts)


class MaterializedDataset(BaseModel):
    address: Address


# TODO: combine with CTEs
# CTE contains procesed query?
# or CTE references CTE?


class ProcessedQuery(BaseModel):
    output_columns: List[BoundConcept]
    ctes: List[CTE | UnionCTE]
    base: CTE | UnionCTE
    # these are just for metadata
    # where_clause: Optional[WhereClause] = None
    # having_clause: Optional[HavingClause] = None
    local_concepts: Annotated[
        BoundEnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=BoundEnvironmentConceptDict)

    @property
    def order_by(self):
        return self.base.order_by
    
    @property
    def limit(self):
        return self.base.limit
    
    @property
    def hidden_columns(self):
        return self.base.hidden_concepts
    
class PersistQueryMixin(BaseModel):
    output_to: MaterializedDataset
    datasource: Datasource
    # base:Dataset


class ProcessedQueryPersist(ProcessedQuery, PersistQueryMixin):
    pass


class CopyQueryMixin(BaseModel):
    target: str
    target_type: IOType
    # base:Dataset


class ProcessedCopyStatement(ProcessedQuery, CopyQueryMixin):
    pass


class ProcessedShowStatement(BaseModel):
    output_columns: List[BoundConcept]
    output_values: List[Union[BoundConcept, Datasource, ProcessedQuery]]


class ProcessedRawSQLStatement(BaseModel):
    text: str


class Limit(BaseModel):
    count: int






class RowsetItem( Namespaced, BaseModel):
    content: BoundConcept
    # rowset: RowsetDerivationStatement
    where: Optional["WhereClause"] = None

    def __repr__(self):
        return (
            f"<Rowset<{self.rowset.name}>: {str(self.content)} where {str(self.where)}>"
        )

    def __str__(self):
        return self.__repr__()
    
    @property
    def arguments(self) -> List[BoundConcept]:
        output = [self.content]
        if self.where:
            output += self.where.input
        return output

    @property
    def output(self) -> BoundConcept:
        return self.content

    @output.setter
    def output(self, value):
        self.content = value

    @property
    def input(self) -> List[BoundConcept]:
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

class Parenthetical(
    ConceptArgs, Namespaced, ConstantInlineable, SelectContext, BaseModel
):
    content: "Expr"

    @property
    def datatype(self):
        return arg_to_datatype(self.content)
    
    @property
    def purpose(self):
        return arg_to_purpose(self.content)
    
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



    def with_select_context(
        self, local_concepts: dict[str, BoundConcept], grain: Grain, environment: BoundEnvironment
    ):
        return Parenthetical(
            content=(
                self.content.with_select_context(local_concepts, grain, environment)
                if isinstance(self.content, SelectContext)
                else self.content
            )
        )

    def inline_constant(self, concept: BoundConcept):
        return Parenthetical(
            content=(
                self.content.inline_constant(concept)
                if isinstance(self.content, ConstantInlineable)
                else self.content
            )
        )

    @property
    def concept_arguments(self) -> List[BoundConcept]:
        base: List[BoundConcept] = []
        x = self.content
        if hasattr(x, "concept_arguments"):
            base += x.concept_arguments
        elif isinstance(x, BoundConcept):
            base.append(x)
        return base

    @property
    def row_arguments(self) -> List[BoundConcept]:
        if isinstance(self.content, ConceptArgs):
            return self.content.row_arguments
        return self.concept_arguments

    @property
    def existence_arguments(self) -> list[tuple["BoundConcept", ...]]:
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




Expr = (
    bool
    | MagicConstants
    | int
    | str
    | float
    | list
    | WindowItem
    | FilterItem
    | BoundConcept
    | Comparison
    | Conditional
    | Parenthetical
    | Function
    | AggregateWrapper
)


BoundConcept.model_rebuild()
BoundMultiSelectStatement.model_rebuild()
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

CTE.model_rebuild()
BaseJoin.model_rebuild()
QueryDatasource.model_rebuild()
ProcessedQuery.model_rebuild()
ProcessedQueryPersist.model_rebuild()
InstantiatedUnnestJoin.model_rebuild()
UndefinedConcept.model_rebuild()
Function.model_rebuild()
Grain.model_rebuild()

