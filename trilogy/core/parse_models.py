from __future__ import annotations


from trilogy.core.models import (
    BoundConcept,
    Reference,
    DatasourceMetadata,
    ColumnAssignment,
    Grain,
    CTE,
    UnionCTE,
    LooseConceptList,
    Namespaced,
    Address,
    validate_concepts,
    SelectContext,
    Metadata,
    HasUUID,
    # Environment,
    # EnvironmentConceptDict,
    address_with_namespace,
        merge_datatypes,
    TupleWrapper,
    OrderItem,
    Function,
    OrderBy,
    CaseWhen,
    CaseElse,
    MapWrapper,
    DataType,
    ListWrapper,
    ListType,
    StructType,
    MapType,
    NumericType,
    DatePart,
    Parenthetical,
    WindowItem,
    WindowItemOver,
    WindowItemOrder,
    FilterItem,
    Comparison,
    Conditional,
    AggregateWrapper,
    RowsetItem,
    WhereClause,
    SubselectComparison,
    Datasource,
    EnvironmentOptions,
    get_version,
    ImportStatement,
    validate_datasources,
    UndefinedConcept,
    arg_to_datatype,
    BoundEnvironment
)
from pydantic import BaseModel
from typing import List
from abc import ABC
from trilogy.core.constants import CONSTANT_DATASET

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



def get_concept_ref_arguments(expr: ExprRef):
    output = []
    if isinstance(expr, ConceptRef) and isinstance(expr, Reference):
        output += [expr]
    elif isinstance(
        expr,
        (
            ComparisonRef,
            ConditionalRef,
            FunctionRef,
            ParentheticalRef,
            AggregateWrapperRef,
            CaseWhenRef,
            CaseElseRef,
        ),
    ):
        output += expr.concept_arguments
    return output


class FunctionRef(Reference, Namespaced, BaseModel):
    operator: FunctionType
    arguments: Sequence[
        Union[
            "AggregateWrapperRef",
            "FunctionRef",
            WindowItemRef,
            int,
            float,
            # str,
            # this must be after a string in parse order
            ConceptRef,
            str,
            date,
            datetime,
            MapWrapper[Any, Any],
            DataType,
            ListType,
            MapType,
            NumericType,
            DatePart,
            "ParentheticalRef",
            CaseWhenRef,
            "CaseElseRef",
            list,
            ListWrapper[Any],
        ]
    ]
    output_datatype: DataType | ListType | StructType | MapType | NumericType | None = None
    output_purpose: Purpose | None = None
    arg_count: int = Field(default=1)
    valid_inputs: Optional[
        Union[
            Set[DataType | ListType | StructType | MapType | NumericType],
            List[Set[DataType | ListType | StructType | MapType | NumericType]],
        ]
    ] = None

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        base = []
        for arg in self.arguments:
            base += get_concept_ref_arguments(arg)
        return base

    def instantiate(self, environment: Environment) -> Function:
        from trilogy.parsing.common import (
            agg_wrapper_to_concept,
            arbitrary_to_concept,
            constant_to_concept,
            filter_item_to_concept,
            function_to_concept,
            process_function_args,
            window_item_to_concept,
            function_args_to_output_purpose,
        )

        from trilogy.core.functions import function_args_to_output_purpose
        
        args = [
            c.instantiate(environment) if isinstance(c, Reference) else c
            for c in self.arguments
        ]
        output_datatype = self.output_datatype
        output_purpose = self.output_purpose
        # args = process_function_args(self.arguments, meta=None, environment=environment)
        
        if not output_datatype:
            output_datatype = merge_datatypes([arg_to_datatype(x, environment) for x in args])
        if not output_purpose:
            output_purpose = function_args_to_output_purpose(args, environment)  
        return Function(
            operator=self.operator,
            arguments=args,
            output_datatype=output_datatype,
            output_purpose=output_purpose,
            valid_inputs=self.valid_inputs,
            arg_count=self.arg_count,
        )

    def with_namespace(self, namespace: str) -> "FunctionRef":
        return FunctionRef(
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


class Concept(Reference, Namespaced, SelectContext, BaseModel):
    name: str
    datatype: DataType | ListType | StructType | MapType | NumericType | None
    purpose: Purpose
    metadata: Metadata = Field(
        default_factory=lambda: Metadata(description=None, line_number=None),
        validate_default=True,
    )
    lineage: Optional[
        Union[
            RowsetItemRef,
            # MultiSelectStatement,
            FunctionRef,
            WindowItemRef,
            FilterItemRef,
            AggregateWrapperRef,
        ]
    ] = None
    grain: Optional[Grain] = None
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    keys: Optional[set[str]] = Field(default_factory=set)
    # grain: "Grain" = Field(default=None, validate_default=True)  # type: ignore
    modifiers: List[Modifier] = Field(default_factory=list)  # type: ignore
    pseudonyms: set[str] = Field(default_factory=set)

    @cached_property
    def address(self) -> str:
        return f"{self.namespace}.{self.name}"

    @property
    def reference(self):
        return ConceptRef(address=self.address)
    
    @property
    def safe_address(self) -> str:
        if self.namespace == DEFAULT_NAMESPACE:
            return self.name.replace(".", "_")
        return self.address.replace(".", "_")

    def instantiate(self, environment: Environment) -> BoundConcept:
        lineage =self.lineage.instantiate(environment) if isinstance(self.lineage, Reference) else self.lineage
        output_datatype = self.datatype
        output_purpose = self.purpose
        if not self.datatype and isinstance(lineage, Function):
            output_datatype = lineage.output_datatype
        if not self.purpose and isinstance(lineage, Function):
            output_purpose = lineage.output_purpose
        return BoundConcept(
            name=self.name,
            datatype=output_datatype,
            purpose=output_purpose,
            metadata=self.metadata,
            lineage = lineage,
            namespace=self.namespace,
            keys=self.keys,
            grain=self.grain,
            modifiers=self.modifiers,
            pseudonyms=self.pseudonyms,
        )

    # def with_select_context(self, environment: Environment) -> BoundConcept:
    #     return BoundConcept(
    #         name=self.name,
    #         datatype=self.datatype,
    #         purpose=self.purpose,
    #         metadata=self.metadata,
    #         lineage=self.lineage.instantiate(environment),
    #         namespace=self.namespace,
    #         keys=self.keys,
    #         grain=self.grain,
    #         modifiers=self.modifiers,
    #         pseudonyms=self.pseudonyms,
    #     )


class ConceptRef(Namespaced, Reference, BaseModel):
    address: str
    line_no: int | None = None

    def __hash__(self):
        return hash(self.address)

    def __init__(self, **data):
        super().__init__(**data)

    @property
    def namespace(self) -> str:
        if "." in self.address:
            return self.address.rsplit(".", 1)[0]
        return DEFAULT_NAMESPACE

    @property
    def name(self) -> str:
        return self.address.split(".")[-1]

    def instantiate(self, environment: Environment) -> Concept:
        base = environment.concepts.__getitem__(self.address, self.line_no)
        if isinstance(base, Reference):
            base = base.instantiate(environment)
        return base

    def with_namespace(self, namespace: str) -> "ConceptRef":
        return ConceptRef(
            address=address_with_namespace(self.address, namespace),
            line_no=self.line_no,
        )


class OrderItemRef(Reference, Namespaced, BaseModel):
    expr: ConceptRef
    order: Ordering

    def with_namespace(self, namespace: str) -> "OrderItemRef":
        return OrderItemRef(expr=self.expr.with_namespace(namespace), order=self.order)

    def instantiate(self, environment: Environment):
        return OrderItem(expr=self.expr.instantiate(environment), order=self.order)


class OrderByRef(Reference, Namespaced, BaseModel):
    items: List[OrderItemRef]

    def with_namespace(self, namespace: str) -> "OrderByRef":
        return OrderByRef(items=[x.with_namespace(namespace) for x in self.items])

    @property
    def concept_arguments(self):
        return [x.expr for x in self.items]

    def instantiate(self, environment: Environment) -> OrderBy:
        return OrderBy(
            items=[
                OrderItem(expr=x.expr.instantiate(environment), order=x.order)
                for x in self.items
            ]
        )


class CaseWhenRef(Reference, Namespaced, BaseModel):
    comparison: ConditionalRef | SubselectComparisonRef | ComparisonRef
    expr: "ExprRef"

    def instantiate(self, environment: Environment):
        return CaseWhen(
            comparison=self.comparison.instantiate(environment),
            expr=(
                self.expr.instantiate(environment)
                if isinstance(self.expr, Reference)
                else self.expr
            ),
        )

    def with_namespace(self, namespace: str) -> CaseWhenRef:
        return CaseWhenRef(
            comparison=self.comparison.with_namespace(namespace),
            expr=(
                self.expr.with_namespace(namespace)
                if isinstance(self.expr, Namespaced)
                else self.expr
            ),
        )


class CaseElseRef(Reference, Namespaced, BaseModel):
    expr: "ExprRef"
    discriminant: ComparisonOperator = ComparisonOperator.ELSE

    def instantiate(self, environment: Environment):
        return CaseElse(
            expr=(
                self.expr.instantiate(environment)
                if isinstance(self.expr, Reference)
                else self.expr
            )
        )

    def with_namespace(self, namespace: str) -> CaseElseRef:
        return CaseElseRef(
            expr=(
                self.expr.with_namespace(namespace)
                if isinstance(self.expr, Namespaced)
                else self.expr
            ),
        )



class RawColumnExpr(BaseModel):
    text: str


class ColumnAssignmentRef(Reference, Namespaced, BaseModel):
    alias: str | RawColumnExpr | FunctionRef
    concept: ConceptRef
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return Modifier.PARTIAL not in self.modifiers

    @property
    def is_nullable(self) -> bool:
        return Modifier.NULLABLE in self.modifiers

    def with_namespace(self, namespace: str) -> "ColumnAssignmentRef":
        return ColumnAssignmentRef(
            alias=(
                self.alias.with_namespace(namespace)
                if isinstance(self.alias, Function)
                else self.alias
            ),
            concept=self.concept.with_namespace(namespace),
            modifiers=self.modifiers,
        )
    
    def instantiate(self, environment:Environment):
        return ColumnAssignment(
            alias = self.alias.instantiate(Environment) if isinstance(self.alias, Reference) else self.alias,
            concept = self.concept.instantiate(environment),
            modifiers = self.modifiers
        )

    # def with_merge(
    #     self, source: BoundConcept, target: BoundConcept, modifiers: List[Modifier]
    # ) -> "ColumnAssignment":
    #     return ColumnAssignment(
    #         alias=self.alias,
    #         concept=self.concept.with_merge(source, target, modifiers),
    #         modifiers=(
    #             modifiers if self.concept.address == source.address else self.modifiers
    #         ),
    #     )



class DatasourceRef(HasUUID, Namespaced, BaseModel):
    name: str
    columns: List[ColumnAssignmentRef]
    address: Union[Address, str]
    grain: Grain = Field(
        default_factory=lambda: Grain(components=set()), validate_default=True
    )
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)
    metadata: DatasourceMetadata = Field(
        default_factory=lambda: DatasourceMetadata(freshness_concept=None)
    )
    where: Optional[WhereClauseRef] = None
    non_partial_for: Optional[WhereClauseRef] = None

    def instantiate(self, environment:Environment)-> Datasource:
        return Datasource(
            name=self.name,
            columns = [x.instantiate(environment).with_grain(self.grain) for x in self.columns],
            address = self.address,
            grain = self.grain,
            namespace = self.namespace,
            metadata = self.metadata,
            where = self.where.instantiate(environment) if self.where else None,
            non_partial_for = self.non_partial_for.instantiate(environment) if self.non_partial_for else None
        )

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


    def add_column(
        self,
        concept: BoundConcept,
        alias: str | RawColumnExpr | Function,
        modifiers: List[Modifier] | None = None,
    ):
        self.columns.append(
            ColumnAssignmentRef(alias=alias, concept=concept, modifiers=modifiers or [])
        )



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
        return DatasourceRef(
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
    def full_concepts(self) -> List[BoundConcept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL not in c.modifiers]

    @property
    def nullable_concepts(self) -> List[BoundConcept]:
        return [c.concept for c in self.columns if Modifier.NULLABLE in c.modifiers]

    @property
    def output_concepts(self) -> List[ConceptRef]:
        return self.concepts

    @property
    def partial_concepts(self) -> List[BoundConcept]:
        return [c.concept for c in self.columns if Modifier.PARTIAL in c.modifiers]

    @property
    def safe_location(self) -> str:
        if isinstance(self.address, Address):
            return self.address.location
        return self.address


class ConditionalRef(Reference, Namespaced, BaseModel):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        ConceptRef,
        ComparisonRef,
        "ConditionalRef",
        "ParentheticalRef",
        FunctionRef,
        FilterItemRef,
    ]
    right: Union[
        int,
        str,
        float,
        list,
        bool,
        MagicConstants,
        ConceptRef,
        ComparisonRef,
        "ConditionalRef",
        "ParentheticalRef",
        FunctionRef,
        FilterItemRef,
    ]
    operator: BooleanOperator

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        output = []
        output += get_concept_ref_arguments(self.left)
        output += get_concept_ref_arguments(self.right)
        return output

    def with_namespace(self, namespace: str) -> "ConditionalRef":
        return ConditionalRef(
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

    def instantiate(self, environment: Environment):
        return Conditional(
            left=(
                self.left.instantiate(environment)
                if isinstance(self.left, Reference)
                else self.left
            ),
            right=(
                self.right.instantiate(environment)
                if isinstance(self.right, Reference)
                else self.right
            ),
            operator=self.operator,
        )


class AggregateWrapperRef(Reference, Namespaced, BaseModel):
    function: FunctionRef
    by: set[ConceptRef] = Field(default_factory=set)

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        base = get_concept_ref_arguments(self.function)
        for x in self.by:
            base += get_concept_ref_arguments(x)
        return base

    def with_namespace(self, namespace: str) -> "AggregateWrapperRef":
        return AggregateWrapperRef(
            function=self.function.with_namespace(namespace),
            by=(
                set([address_with_namespace(c, namespace) for c in self.by])
                if self.by
                else set()
            ),
        )

    def instantiate(self, environment):
        return AggregateWrapper(
            function=self.function.instantiate(environment),
            by=set([c.instantiate(environment) for c in self.by]) if self.by else set(),
        )


class WhereClauseRef(Reference, Namespaced, BaseModel):
    conditional: Union[
        SubselectComparisonRef, ComparisonRef, ConditionalRef, "ParentheticalRef"
    ]

    @property
    def arguments(self):
        return get_concept_ref_arguments(self.conditional)

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        return get_concept_ref_arguments(self.conditional)

    def with_namespace(self, namespace: str) -> WhereClauseRef:
        return WhereClauseRef(conditional=self.conditional.with_namespace(namespace))

    def instantiate(self, environment):
        return WhereClause(conditional=self.conditional.instantiate(environment))


class HavingClauseRef(WhereClauseRef):
    pass


class RowsetItemRef(Reference, Namespaced, BaseModel):
    content: ConceptRef
    rowset: RowsetDerivationStatement
    where: Optional[WhereClauseRef] = None

    @property
    def arguments(self) -> List[ConceptRef]:
        output = [self.content]
        if self.where:
            output += get_concept_ref_arguments(self.where)
        return output

    def with_namespace(self, namespace: str) -> "RowsetItemRef":
        return RowsetItemRef(
            content=self.content.with_namespace(namespace),
            where=self.where.with_namespace(namespace) if self.where else None,
            rowset=self.rowset.with_namespace(namespace),
        )

    def instantiate(self, environment: Environment) -> RowsetItem:
        return RowsetItem(
            content=self.content.instantiate(environment),
            where=self.where.instantiate(environment) if self.where else None,
            rowset=self.rowset,
        )


class ParentheticalRef(Reference, Namespaced, BaseModel):
    content: "ExprRef"

    def with_namespace(self, namespace: str) -> "ParentheticalRef":
        return ParentheticalRef(content=self.content.with_namespace(namespace))

    def instantiate(self, environment: Environment):
        return Parenthetical(content=self.content.instantiate(environment))


class WindowItemOverRef(Reference, BaseModel):
    contents: List[ConceptRef]

    def instantiate(self, environment: Environment) -> WindowItemOver:
        return WindowItemOver(
            contents=[c.instantiate(environment) for c in self.contents]
        )


class WindowItemOrderRef(Reference, BaseModel):
    contents: List[OrderItemRef]

    def instantiate(self, environment: Environment) -> WindowItemOrder:
        return WindowItemOrder(
            contents=[c.instantiate(environment) for c in self.contents]
        )


class WindowItemRef(Reference, Namespaced, BaseModel):
    type: WindowType
    content: ConceptRef
    order_by: List["OrderItemRef"]
    over: List["ConceptRef"]
    index: Optional[int] = None

    @property
    def arguments(self):
        base = [self.content]
        for order in self.order_by:
            base += [order.expr]
        for item in self.over:
            base += [item]
        return base

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        base = get_concept_ref_arguments(self.content)
        for order in self.order_by:
            base += get_concept_ref_arguments(order.expr)
        for item in self.over:
            base += get_concept_ref_arguments(item)
        return

    def with_namespace(self, namespace: str) -> "WindowItemRef":
        return WindowItemRef(
            type=self.type,
            content=self.content.with_namespace(namespace),
            over=[x.with_namespace(namespace) for x in self.over],
            order_by=[x.with_namespace(namespace) for x in self.order_by],
            index=self.index,
        )

    def instantiate(self, environment: Environment) -> WindowItem:
        return WindowItem(
            type=self.type,
            content=self.content.instantiate(environment),
            over=[x.instantiate(environment) for x in self.over],
            order_by=[x.instantiate(environment) for x in self.order_by],
            index=self.index,
        )


class FilterItemRef(Reference, Namespaced, BaseModel):
    content: ConceptRef
    where: "WhereClauseRef"

    @property
    def arguments(self):
        base = [self.content]
        base += self.where.arguments
        return base

    @property
    def concept_arguments(self):
        return [self.content] + self.where.concept_arguments

    def with_namespace(self, namespace: str) -> "FilterItemRef":
        return FilterItemRef(
            content=self.content.with_namespace(namespace),
            where=self.where.with_namespace(namespace),
        )

    def instantiate(self, environment):
        return FilterItem(
            content=self.content.instantiate(environment),
            where=self.where.instantiate(environment),
        )


class ComparisonRef(Reference, Namespaced, BaseModel):
    left: Union[
        int,
        str,
        float,
        list,
        bool,
        datetime,
        date,
        FunctionRef,
        ConceptRef,
        "ConditionalRef",
        DataType,
        "ComparisonRef",
        "ParentheticalRef",
        MagicConstants,
        WindowItemRef,
        AggregateWrapperRef,
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
        FunctionRef,
        "ConditionalRef",
        DataType,
        "ComparisonRef",
        "ParentheticalRef",
        MagicConstants,
        WindowItemRef,
        AggregateWrapperRef,
        TupleWrapper,
    ]
    operator: ComparisonOperator

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        return get_concept_ref_arguments(self.left) + get_concept_ref_arguments(
            self.right
        )

    def instantiate(self, environment: Environment):
        return Comparison(
            left=(
                self.left.instantiate(environment)
                if isinstance(self.left, Reference)
                else self.left
            ),
            right=(
                self.right.instantiate(environment)
                if isinstance(self.right, Reference)
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


class SubselectComparisonRef(ComparisonRef):

    def instantiate(self, environment: Environment):
        return SubselectComparison(
            left=(
                self.left.instantiate(environment)
                if isinstance(self.left, Reference)
                else self.left
            ),
            right=(
                self.right.instantiate(environment)
                if isinstance(self.right, Reference)
                else self.right
            ),
            operator=self.operator,
        )


class ConceptDeclarationStatement(HasUUID, BaseModel):
    concept: Concept


class ConceptDerivation(BaseModel):
    concept: Concept


class EnvironmentConceptDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)
        self.undefined: dict[str, UndefinedConcept] = {}
        self.fail_on_missing: bool = True
        self.populate_default_concepts()

    def duplicate(self) -> "EnvironmentConceptDict":
        new = EnvironmentConceptDict()
        new.update({k: v.duplicate() for k, v in self.items()})
        new.undefined = self.undefined
        new.fail_on_missing = self.fail_on_missing
        return new

    def populate_default_concepts(self):
        from trilogy.core.internal import DEFAULT_CONCEPTS

        for concept in DEFAULT_CONCEPTS.values():
            self[concept.address] = concept

    def values(self) -> ValuesView[Concept]:  # type: ignore
        return super().values()

    def get(self, key: str, default: Concept | None = None) -> Concept | None:  # type: ignore
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
    ) -> Concept | UndefinedConcept:
        try:
            return super(EnvironmentConceptDict, self).__getitem__(key)
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

    def items(self) -> ItemsView[str, Concept]:  # type: ignore
        return super().items()


class ConceptTransform(Namespaced, BaseModel):
    function: FunctionRef | FilterItemRef | WindowItemRef | AggregateWrapperRef
    output: ConceptRef
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def input(self) -> List[ConceptRef]:
        return [v for v in self.function.arguments if isinstance(v, ConceptRef)]

    def with_namespace(self, namespace: str) -> "ConceptTransform":
        return ConceptTransform(
            function=self.function.with_namespace(namespace),
            output=self.output.with_namespace(namespace),
            modifiers=self.modifiers,
        )


class SelectItem(Namespaced, BaseModel):
    content: Union[ConceptRef, ConceptTransform]
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def output(self) -> Concept:
        if isinstance(self.content, ConceptTransform):
            return self.content.output
        elif isinstance(self.content, WindowItem):
            return self.content.output
        return self.content

    @property
    def input(self) -> List[ConceptRef]:
        return self.content.input

    def with_namespace(self, namespace: str) -> "SelectItem":
        return SelectItem(
            content=self.content.with_namespace(namespace),
            modifiers=self.modifiers,
        )


ExprRef = (
    bool
    | MagicConstants
    | int
    | str
    | float
    | list
    | WindowItemRef
    | FilterItemRef
    | ConceptRef
    | ComparisonRef
    | ConditionalRef
    | ParentheticalRef
    | FunctionRef
    | AggregateWrapperRef
)


class SelectTypeMixin(BaseModel):
    where_clause: Union["WhereClauseRef", None] = Field(default=None)
    having_clause: Union["HavingClauseRef", None] = Field(default=None)

    @property
    def output_components(self) -> List[Concept]:
        raise NotImplementedError

    @property
    def implicit_where_clause_selections(self) -> List[Concept]:
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


class SelectStatement(HasUUID, Namespaced, SelectTypeMixin, BaseModel):
    selection: List[SelectItem]
    order_by: Optional[OrderByRef] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    local_concepts: Annotated[
        EnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=EnvironmentConceptDict)

    @classmethod
    def from_inputs(
        cls,
        environment: Environment,
        selection: List[SelectItem],
        order_by: OrderByRef | None = None,
        limit: int | None = None,
        meta: Metadata | None = None,
        where_clause: WhereClauseRef | None = None,
        having_clause: HavingClauseRef | None = None,
    ) -> "SelectStatement":

        output = SelectStatement(
            selection=selection,
            where_clause=where_clause,
            having_clause=having_clause,
            limit=limit,
            order_by=order_by,
            meta=meta or Metadata(),
        )

        output.validate_syntax(environment)
        return output

    def validate_syntax(self, environment: Environment):
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

    @field_validator("selection", mode="before")
    @classmethod
    def selection_validation(cls, v):
        new = []
        for item in v:
            if isinstance(item, (Concept, ConceptTransform)):
                new.append(SelectItem(content=item))
            else:
                new.append(item)
        return new

    @property
    def locally_derived(self) -> set[str]:
        locally_derived: set[str] = set()
        for item in self.selection:
            if isinstance(item.content, ConceptTransform):
                locally_derived.add(item.content.output.address)
        return locally_derived

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
    def output_components(self) -> List[ConceptRef]:
        output = []
        for item in self.selection:
            if isinstance(item, ConceptRef):
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

    def to_datasource(
        self,
        namespace: str,
        name: str,
        address: Address,
        environment: Environment,
        grain: Grain | None = None,
    ) -> DatasourceRef:
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

    def with_namespace(self, namespace: str) -> "SelectStatement":
        return SelectStatement(
            selection=[c.with_namespace(namespace) for c in self.selection],
            where_clause=(
                self.where_clause.with_namespace(namespace)
                if self.where_clause
                else None
            ),
            order_by=self.order_by.with_namespace(namespace) if self.order_by else None,
            limit=self.limit,
        )


class CopyStatement(BaseModel):
    target: str
    target_type: IOType
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())
    select: SelectStatement


class MultiSelectStatement(HasUUID, SelectTypeMixin, Namespaced, BaseModel):
    selects: List[SelectStatement]
    align: AlignClause
    namespace: str
    order_by: Optional[OrderByRef] = None
    limit: Optional[int] = None
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())
    local_concepts: Annotated[
        EnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=EnvironmentConceptDict)

    def __repr__(self):
        return "MultiSelect<" + " MERGE ".join([str(s) for s in self.selects]) + ">"

    @property
    def arguments(self) -> List[ConceptRef]:
        output = []
        for select in self.selects:
            output += select.input_components
        return unique(output, "address")

    @property
    def concept_arguments(self) -> List[ConceptRef]:
        output = []
        for select in self.selects:
            output += select.input_components
        if self.where_clause:
            output += self.where_clause.concept_arguments
        return unique(output, "address")

    def get_merge_concept(self, check: Concept):
        for item in self.align.items:
            if check in item.concepts_lcl:
                return item.gen_concept(self)
        return None

    def with_namespace(self, namespace: str) -> "MultiSelectStatement":
        return MultiSelectStatement(
            selects=[c.with_namespace(namespace) for c in self.selects],
            align=self.align.with_namespace(namespace),
            namespace=namespace,
            order_by=self.order_by.with_namespace(namespace) if self.order_by else None,
            limit=self.limit,
            meta=self.meta,
            where_clause=(
                self.where_clause.with_namespace(namespace)
                if self.where_clause
                else None
            ),
            local_concepts=EnvironmentConceptDict(
                {k: v.with_namespace(namespace) for k, v in self.local_concepts.items()}
            ),
        )

    @computed_field  # type: ignore
    @cached_property
    def derived_concepts(self) -> List[ConceptRef]:
        output = []
        for item in self.align.items:
            output.append(item.gen_concept(self))
        return output

    def find_source(self, concept: Concept, cte: CTE | UnionCTE) -> Concept:
        for x in self.align.items:
            if concept.name == x.alias:
                for c in x.concepts:
                    if c.address in cte.output_lcl:
                        return c
        raise SyntaxError(
            f"Could not find upstream map for multiselect {str(concept)} on cte ({cte})"
        )

    @property
    def output_components(self) -> List[ConceptRef]:
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


class AlignItem(Namespaced, BaseModel):
    alias: str
    concepts: List[Concept]
    namespace: Optional[str] = Field(default=DEFAULT_NAMESPACE, validate_default=True)

    @computed_field  # type: ignore
    @cached_property
    def concepts_lcl(self) -> LooseConceptList:
        return LooseConceptList(concepts=self.concepts)

    def with_namespace(self, namespace: str) -> "AlignItem":
        return AlignItem(
            alias=self.alias,
            concepts=[c.with_namespace(namespace) for c in self.concepts],
            namespace=namespace,
        )

    def gen_concept(self, parent: MultiSelectStatement):
        datatypes = set([c.datatype for c in self.concepts])
        purposes = set([c.purpose for c in self.concepts])
        if len(datatypes) > 1:
            raise InvalidSyntaxException(
                f"Datatypes do not align for merged statements {self.alias}, have {datatypes}"
            )
        if len(purposes) > 1:
            purpose = Purpose.KEY
        else:
            purpose = list(purposes)[0]
        new = Concept(
            name=self.alias,
            datatype=datatypes.pop(),
            purpose=purpose,
            lineage=parent,
            namespace=parent.namespace,
        )
        return new


class AlignClause(Namespaced, BaseModel):
    items: List[AlignItem]

    def with_namespace(self, namespace: str) -> "AlignClause":
        return AlignClause(items=[x.with_namespace(namespace) for x in self.items])


class RawSQLStatement(BaseModel):
    text: str
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())


class MergeStatementV2(HasUUID, Namespaced, BaseModel):
    sources: list[Concept]
    targets: dict[str, Concept]
    source_wildcard: str | None = None
    target_wildcard: str | None = None
    modifiers: List[Modifier] = Field(default_factory=list)

    def with_namespace(self, namespace: str) -> "MergeStatementV2":
        new = MergeStatementV2(
            sources=[x.with_namespace(namespace) for x in self.sources],
            targets={k: v.with_namespace(namespace) for k, v in self.targets.items()},
            modifiers=self.modifiers,
        )
        return new



class PersistStatement(HasUUID, BaseModel):
    datasource: DatasourceRef
    select: SelectStatement
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())

    @property
    def identifier(self):
        return self.datasource.identifier

    @property
    def address(self):
        return self.datasource.address


class ShowStatement(BaseModel):
    content: SelectStatement | PersistStatement | ShowCategory


class RowsetDerivationStatement(HasUUID, Namespaced, BaseModel):
    name: str
    select: SelectStatement | MultiSelectStatement
    namespace: str

    def __repr__(self):
        return f"RowsetDerivation<{str(self.select)}>"

    def __str__(self):
        return self.__repr__()

    def create_derived_concepts(
        self, environment: Environment, concrete: bool = False
    ) -> List[Concept]:
        output: list[Concept] = []
        orig: dict[str, Concept] = {}
        for orig_concept in self.select.output_components:
            orig_concept = orig_concept.instantiate(environment)
            name = orig_concept.name
            if isinstance(orig_concept.lineage, FilterItem):
                if orig_concept.lineage.where == self.select.where_clause:
                    name = orig_concept.lineage.content.name

            new_concept = Concept(
                name=name,
                datatype=orig_concept.datatype,
                purpose=orig_concept.purpose,
                lineage=(
                    RowsetItem(
                        content=orig_concept,
                        where=(
                            self.select.where_clause.instantiate(environment)
                            if self.select.where_clause
                            else None
                        ),
                        rowset=self,
                    )
                    if concrete
                    else RowsetItemRef(
                        content=orig_concept.reference,
                        where=self.select.where_clause,
                        rowset=self,
                    )
                ),
                grain=orig_concept.grain,
                # TODO: add proper metadata
                metadata=Metadata(concept_source=ConceptSource.CTE),
                namespace=(
                    f"{self.name}.{orig_concept.namespace}"
                    if orig_concept.namespace != self.namespace
                    else self.name
                ),
                keys=orig_concept.keys,
            )
            orig[orig_concept.address] = new_concept
            output.append(new_concept)
        default_grain = Grain.from_concepts([*output])
        # remap everything to the properties of the rowset
        for x in output:
            if x.keys:
                if all([k in orig for k in x.keys]):
                    x.keys = set([orig[k].address if k in orig else k for k in x.keys])
                else:
                    # TODO: fix this up
                    x.keys = set()
        for x in output:
            if all([c in orig for c in x.grain.components]):
                x.grain = Grain(
                    components={orig[c].address for c in x.grain.components}
                )
            else:
                x.grain = default_grain
        return output

    @property
    def arguments(self) -> List[Concept]:
        return self.select.output_components

    def with_namespace(self, namespace: str) -> "RowsetDerivationStatement":
        return RowsetDerivationStatement(
            name=self.name,
            select=self.select.with_namespace(namespace),
            namespace=namespace,
        )


SelectStatement.model_rebuild()



class EnvironmentDatasourceDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)

    def __getitem__(self, key: str) -> DatasourceRef:
        try:
            return super(EnvironmentDatasourceDict, self).__getitem__(key)
        except KeyError:
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key)
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1])
            raise

    def values(self) -> ValuesView[DatasourceRef]:  # type: ignore
        return super().values()

    def items(self) -> ItemsView[str, DatasourceRef]:  # type: ignore
        return super().items()

    def duplicate(self) -> "EnvironmentDatasourceDict":
        new = EnvironmentDatasourceDict()
        new.update({k: v.duplicate() for k, v in self.items()})
        return new



class Environment(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, strict=False)

    concepts: Annotated[EnvironmentConceptDict, PlainValidator(validate_concepts)] = (
        Field(default_factory=EnvironmentConceptDict)
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
    alias_origin_lookup: Dict[str, Concept] = Field(default_factory=dict)
    # TODO: support freezing environments to avoid mutation
    frozen: bool = False
    env_file_path: Path | None = None

    def instantiate(self)->BoundEnvironment:
        env = BoundEnvironment()
        for k, v in self.concepts.items():
            env.concepts[k] = v.instantiate(self)
        for k, v in self.datasources.items():
            env.datasources[k] = v.instantiate(self)
        env.alias_origin_lookup = {k:v.instantiate(self) for k, v in self.alias_origin_lookup.items()}
        env.cte_name_map = self.cte_name_map


        env.gen_concept_list_caches()
        return env


    def freeze(self):
        self.frozen = True

    def thaw(self):
        self.frozen = False

    def duplicate(self):
        return Environment.model_construct(
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
    def from_file(cls, path: str | Path) -> "Environment":
        if isinstance(path, str):
            path = Path(path)
        with open(path, "r") as f:
            read = f.read()
        return Environment(working_path=path.parent, env_file_path=path).parse(read)[0]

    @classmethod
    def from_string(cls, input: str) -> "Environment":
        return Environment().parse(input)[0]

    @classmethod
    def from_cache(cls, path) -> Optional["Environment"]:
        with open(path, "r") as f:
            read = f.read()
        base = cls.model_validate_json(read)
        version = get_version()
        if base.version != version:
            return None
        return base

    def to_cache(self, path: Optional[str | Path] = None) -> Path:
        if not path:
            ppath = Path(self.working_path) / ENV_CACHE_NAME
        else:
            ppath = Path(path)
        with open(ppath, "w") as f:
            f.write(self.model_dump_json())
        return ppath

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

    def validate_concept(self, new_concept: Concept, meta: Meta | None = None):
        lookup = new_concept.address
        existing: Concept = self.concepts.get(lookup)  # type: ignore
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

    def add_import(
        self, alias: str, source: Environment, imp_stm: ImportStatement | None = None
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        exists = False
        existing = self.imports[alias]
        if imp_stm:
            if any(
                [x.path == imp_stm.path and x.alias == imp_stm.alias for x in existing]
            ):
                exists = True
        else:
            if any(
                [x.path == source.working_path and x.alias == alias for x in existing]
            ):
                exists = True
            imp_stm = ImportStatement(alias=alias, path=Path(source.working_path))
        same_namespace = alias == self.namespace

        if not exists:
            self.imports[alias].append(imp_stm)
        # we can't exit early
        # as there may be new concepts
        for k, concept in source.concepts.items():
            # skip internal namespace
            if INTERNAL_NAMESPACE in concept.address:
                continue
            if same_namespace:
                new = self.add_concept(concept, _ignore_cache=True)
            else:
                new = self.add_concept(
                    concept.with_namespace(alias), _ignore_cache=True
                )

                k = address_with_namespace(k, alias)
            # set this explicitly, to handle aliasing
            self.concepts[k] = new

        for _, datasource in source.datasources.items():
            if same_namespace:
                self.add_datasource(datasource, _ignore_cache=True)
            else:
                self.add_datasource(
                    datasource.with_namespace(alias), _ignore_cache=True
                )
        for key, val in source.alias_origin_lookup.items():
            if same_namespace:
                self.alias_origin_lookup[key] = val
            else:
                self.alias_origin_lookup[address_with_namespace(key, alias)] = (
                    val.with_namespace(alias)
                )

        self.gen_concept_list_caches()
        return self

    def add_file_import(
        self, path: str | Path, alias: str, env: Environment | None = None
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        from trilogy.parsing.parse_engine import (
            PARSER,
            ParseToObjects,
            gen_cache_lookup,
        )

        if isinstance(path, str):
            if path.endswith(".preql"):
                path = path.rsplit(".", 1)[0]
            if "." not in path:
                target = Path(self.working_path, path)
            else:
                target = Path(self.working_path, *path.split("."))
            target = target.with_suffix(".preql")
        else:
            target = path
        if not env:
            parse_address = gen_cache_lookup(str(target), alias, str(self.working_path))
            try:
                with open(target, "r", encoding="utf-8") as f:
                    text = f.read()
                nenv = Environment(
                    working_path=target.parent,
                )
                nenv.concepts.fail_on_missing = False
                nparser = ParseToObjects(
                    environment=Environment(
                        working_path=target.parent,
                    ),
                    parse_address=parse_address,
                    token_address=target,
                )
                nparser.set_text(text)
                nparser.transform(PARSER.parse(text))
                nparser.hydrate_missing()

            except Exception as e:
                raise ImportError(
                    f"Unable to import file {target.parent}, parsing error: {e}"
                )
            env = nparser.environment
        imps = ImportStatement(alias=alias, path=target)
        self.add_import(alias, source=env, imp_stm=imps)
        return imps

    def parse(
        self, input: str, namespace: str | None = None, persist: bool = False
    ) -> Tuple[Environment, list]:
        raise NotImplementedError
        # from trilogy import parse
        # from trilogy.core.query_processor import process_persist

        # if namespace:
        #     new = Environment()
        #     _, queries = new.parse(input)
        #     self.add_import(namespace, new)
        #     return self, queries
        # _, queries = parse(input, self)
        # generatable = [
        #     x
        #     for x in queries
        #     if isinstance(
        #         x,
        #         (
        #             SelectStatement,
        #             PersistStatement,
        #             MultiSelectStatement,
        #             ShowStatement,
        #         ),
        #     )
        # ]
        # while generatable:
        #     t = generatable.pop(0)
        #     if isinstance(t, PersistStatement) and persist:
        #         processed = process_persist(self, t)
        #         self.add_datasource(processed.datasource)
        # return self, queries

    def add_concept(
        self,
        concept: Concept,
        meta: Meta | None = None,
        force: bool = False,
        add_derived: bool = True,
        _ignore_cache: bool = False,
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add concepts")
        if not force:
            existing = self.validate_concept(concept, meta=meta)
            if existing:
                concept = existing
        self.concepts[concept.address] = concept
        from trilogy.core.environment_helpers import generate_related_concepts

        # generate_related_concepts(concept, self, meta=meta, add_derived=add_derived)
        # TODO: reenable
        if not _ignore_cache:
            self.gen_concept_list_caches()
        return concept

    def add_datasource(
        self,
        datasource: DatasourceRef,
        meta: Meta | None = None,
        _ignore_cache: bool = False,
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add datasource")
        self.datasources[datasource.identifier] = datasource

        eligible_to_promote_roots = datasource.non_partial_for is None
        # mark this as canonical source
        for concept_ref in datasource.output_concepts:
            current_concept = self.concepts[concept_ref.address]
            if not eligible_to_promote_roots:
                continue

            # current_derivation = current_concept.derivation
            # TODO: refine this section;
            # too hacky for maintainability

            if current_concept.lineage:
                persisted = f"{PERSISTED_CONCEPT_PREFIX}_" + current_concept.name
                # override the current concept source to reflect that it's now coming from a datasource
                if (
                    current_concept.metadata.concept_source
                    != ConceptSource.PERSIST_STATEMENT
                ):
                    new_concept = current_concept.model_copy(deep=True)
                    new_concept.set_name(persisted)
                    self.add_concept(
                        new_concept, meta=meta, force=True, _ignore_cache=True
                    )
                    current_concept.metadata.concept_source = (
                        ConceptSource.PERSIST_STATEMENT
                    )
                    # remove the associated lineage
                    # to make this a root for discovery purposes
                    # as it now "exists" in a table
                    current_concept.lineage = None
                    current_concept = current_concept.with_default_grain()
                    self.add_concept(
                        current_concept, meta=meta, force=True, _ignore_cache=True
                    )
                    self.merge_concept(new_concept, current_concept, [])
                else:
                    self.add_concept(current_concept, meta=meta, _ignore_cache=True)
        if not _ignore_cache:
            self.gen_concept_list_caches()
        return datasource

    def delete_datasource(
        self,
        address: str,
        meta: Meta | None = None,
    ) -> bool:
        if self.frozen:
            raise ValueError("Environment is frozen, cannot delete datsources")
        if address in self.datasources:
            del self.datasources[address]
            self.gen_concept_list_caches()
            return True
        return False

    def merge_concept(
        self,
        source: Concept,
        target: Concept,
        modifiers: List[Modifier],
        force: bool = False,
    ) -> bool:
        if self.frozen:
            raise ValueError("Environment is frozen, cannot merge concepts")
        replacements = {}

        # exit early if we've run this
        if source.address in self.alias_origin_lookup and not force:
            if self.concepts[source.address] == target:
                return False
        self.alias_origin_lookup[source.address] = source
        for k, v in self.concepts.items():
            if v.address == target.address:
                v.pseudonyms.add(source.address)

            if v.address == source.address:
                replacements[k] = target
                v.pseudonyms.add(target.address)
            # we need to update keys and grains of all concepts
            else:
                replacements[k] = v.with_merge(source, target, modifiers)
        self.concepts.update(replacements)

        for k, ds in self.datasources.items():
            if source.address in ds.output_lcl:
                ds.merge_concept(source, target, modifiers=modifiers)
        return True