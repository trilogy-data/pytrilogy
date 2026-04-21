from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import List, Literal, Mapping, Optional, Union

from trilogy.constants import CONFIG, DEFAULT_NAMESPACE
from trilogy.core.enums import (
    ChartType,
    ConceptSource,
    CreateMode,
    FunctionClass,
    IOType,
    Modifier,
    PersistMode,
    PublishAction,
    ShowCategory,
    ValidationScope,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    ArgBinding,
    Concept,
    ConceptRef,
    CustomType,
    DeriveClause,
    Expr,
    FilterItem,
    Function,
    FunctionCallWrapper,
    Grain,
    HasUUID,
    HavingClause,
    Metadata,
    MultiSelectLineage,
    OrderBy,
    Parenthetical,
    SelectLineage,
    SubselectItem,
    UndefinedConcept,
    WhereClause,
    WindowItem,
)
from trilogy.core.models.datasource import Address, ColumnAssignment, Datasource
from trilogy.core.models.environment import (
    Environment,
    EnvironmentConceptDict,
    validate_concepts,
)
from trilogy.core.statements.common import SelectTypeMixin
from trilogy.utility import unique


@dataclass
class ConceptTransform:
    function: (
        Function
        | FilterItem
        | WindowItem
        | AggregateWrapper
        | FunctionCallWrapper
        | Parenthetical
        | SubselectItem
    )
    output: Concept  # this has to be a full concept, as it may not exist in environment
    modifiers: List[Modifier] = field(default_factory=list)

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


@dataclass
class SelectItem:
    content: Union[ConceptTransform, ConceptRef]
    modifiers: List[Modifier] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.content, Concept):
            self.content = self.content.reference

    @property
    def concept(self) -> ConceptRef:
        if isinstance(self.content, ConceptRef):
            return self.content
        elif isinstance(self.content, Concept):
            return self.content.reference
        return self.content.output.reference

    @property
    def is_undefined(self) -> bool:
        return True if isinstance(self.content, UndefinedConcept) else False


@dataclass
class FromClause:
    sources: List[str]


@dataclass
class SelectStatement(HasUUID, SelectTypeMixin):
    selection: List[SelectItem]
    where_clause: Optional[WhereClause] = None
    having_clause: Optional[HavingClause] = None
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    eligible_datasources: Optional[list[str]] = None
    meta: Metadata = field(default_factory=Metadata)
    local_concepts: EnvironmentConceptDict = field(
        default_factory=EnvironmentConceptDict
    )
    grain: Grain = field(default_factory=Grain)

    def __post_init__(self):
        new = []
        for item in self.selection:
            if isinstance(item, (Concept, ConceptTransform)):
                new.append(SelectItem(content=item))
            else:
                new.append(item)
        self.selection = new
        if not isinstance(self.local_concepts, EnvironmentConceptDict):
            self.local_concepts = validate_concepts(self.local_concepts)

    def as_lineage(self, environment: Environment) -> SelectLineage:
        derived = [
            x.concept.address
            for x in self.selection
            if isinstance(x.content, ConceptTransform)
        ]
        return SelectLineage(
            selection=[
                environment.concepts[x.concept.address].reference
                for x in self.selection
            ],
            order_by=self.order_by,
            limit=self.limit,
            where_clause=self.where_clause,
            having_clause=self.having_clause,
            local_concepts={
                k: v for k, v in self.local_concepts.items() if k in derived
            },
            hidden_components=self.hidden_components,
            grain=self.grain,
        )

    @classmethod
    def from_inputs(
        cls,
        environment: Environment,
        selection: List[SelectItem],
        order_by: OrderBy | None = None,
        limit: int | None = None,
        meta: Metadata | None = None,
        where_clause: WhereClause | None = None,
        having_clause: HavingClause | None = None,
        eligible_datasources: list[str] | None = None,
    ) -> "SelectStatement":
        output = SelectStatement(
            selection=selection,
            where_clause=where_clause,
            having_clause=having_clause,
            limit=limit,
            order_by=order_by,
            meta=meta or Metadata(),
            eligible_datasources=eligible_datasources,
        )

        output.grain = output.calculate_grain(environment, output.local_concepts)
        output_addresses = set()
        for x in selection:
            if x.is_undefined and environment.concepts.fail_on_missing:
                environment.concepts.raise_undefined(
                    x.concept.address, meta.line_number if meta else None
                )
            elif isinstance(x.content, ConceptTransform):
                if isinstance(x.content.output, UndefinedConcept):
                    continue
                if CONFIG.parsing.select_as_definition and not environment.frozen:
                    if x.concept.address not in environment.concepts:
                        environment.add_concept(x.content.output)
                    elif x.concept.address in environment.concepts:
                        version = environment.concepts[x.concept.address]
                        if version.metadata.concept_source == ConceptSource.SELECT:
                            environment.add_concept(x.content.output, force=True)
                x.content.output = x.content.output.set_select_grain(
                    output.grain, environment
                )
                # we might not need this
                output.local_concepts[x.content.output.address] = x.content.output
                if x.content.output.address in output_addresses:
                    raise SyntaxError(
                        f"Duplicate select output for {x.content.output.address}; Line: {meta.line_number if meta else 'unknown'}"
                    )
                output_addresses.add(x.content.output.address)
            elif isinstance(x.content, ConceptRef):
                output.local_concepts[x.content.address] = environment.concepts[
                    x.content.address
                ]
                if x.content.address in output_addresses:
                    raise SyntaxError(
                        f"Duplicate select output for {x.content.address}; Line: {meta.line_number if meta else 'unknown'}"
                    )
                output_addresses.add(x.content.address)
        output.grain = output.calculate_grain(environment, output.local_concepts)
        output.validate_syntax(environment)
        return output

    def calculate_grain(
        self,
        environment: Environment | None = None,
        local_concepts: Mapping[str, Concept] | None = None,
    ) -> Grain:
        targets = []
        for x in self.selection:
            targets.append(x.concept)

        result = Grain.from_concepts(
            targets,
            where_clause=self.where_clause,
            environment=environment,
            local_concepts=local_concepts,
        )
        return result

    def validate_syntax(self, environment: Environment):
        if self.where_clause:
            for x in self.where_clause.concept_arguments:
                if isinstance(x, UndefinedConcept):
                    validate = environment.concepts.get(x.address)
                    if validate and self.where_clause:
                        self.where_clause = (
                            self.where_clause.with_reference_replacement(
                                x.address, validate.reference
                            )
                        )
                    else:
                        environment.concepts.raise_undefined(
                            x.address, x.metadata.line_number if x.metadata else None
                        )
        all_in_output = [x for x in self.output_components]
        if self.where_clause:
            for cref in self.where_clause.concept_arguments:
                concept = environment.concepts[cref.address]
                if isinstance(concept, UndefinedConcept):
                    continue
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
            for cref in self.having_clause.concept_arguments:
                if cref.address not in [x for x in self.output_components]:
                    raise SyntaxError(
                        f"Cannot reference a column ({cref.address}) that is not in the select projection in the HAVING clause, move to WHERE;  Line: {self.meta.line_number}"
                    )
        if self.order_by:
            for cref in self.order_by.concept_arguments:
                if cref.address not in all_in_output:
                    raise SyntaxError(
                        f"Cannot order by column {cref.address} that is not in the output projection; line: {self.meta.line_number}"
                    )

    def __str__(self):
        from trilogy.parsing.render import render_query

        return render_query(self)

    @property
    def locally_derived(self) -> set[str]:
        locally_derived: set[str] = set()
        for item in self.selection:
            if isinstance(item.content, ConceptTransform):
                locally_derived.add(item.concept.address)
        return locally_derived

    @property
    def output_components(self) -> List[ConceptRef]:
        return [x.concept for x in self.selection]

    @property
    def hidden_components(self) -> set[str]:
        return set(
            x.concept.address for x in self.selection if Modifier.HIDDEN in x.modifiers
        )

    def to_datasource(
        self,
        namespace: str,
        name: str,
        address: Address,
        environment: Environment,
        grain: Grain | None = None,
    ) -> Datasource:
        if self.where_clause or self.having_clause:
            modifiers = [Modifier.PARTIAL]
        else:
            modifiers = []
        columns = [
            # TODO: replace hardcoded replacement here
            # if the concept is a locally derived concept, it cannot ever be partial
            # but if it's a concept pulled in from upstream and we have a where clause, it should be partial
            ColumnAssignment(
                alias=(
                    c.address.replace(".", "_")
                    if c.namespace != DEFAULT_NAMESPACE
                    else c.name
                ),
                concept=environment.concepts[c.address].reference,
                modifiers=modifiers if c.address not in self.locally_derived else [],
            )
            for c in self.output_components
        ]

        condition = None
        if self.where_clause:
            condition = self.where_clause.conditional
        if self.having_clause:
            if condition:
                condition = self.having_clause.conditional + condition
            else:
                condition = self.having_clause.conditional

        new_datasource = Datasource(
            name=name,
            address=address,
            grain=grain or self.grain,
            columns=columns,
            namespace=namespace,
            non_partial_for=WhereClause(conditional=condition) if condition else None,
        )
        return new_datasource


@dataclass
class RawSQLStatement:
    text: str
    meta: Optional[Metadata] = field(default_factory=Metadata)


@dataclass
class CopyStatement:
    target: str
    target_type: IOType
    select: SelectStatement
    meta: Optional[Metadata] = field(default_factory=Metadata)


@dataclass
class MultiSelectStatement(HasUUID, SelectTypeMixin):
    selects: List[SelectStatement]
    align: AlignClause
    namespace: str
    derived_concepts: List[Concept]
    where_clause: Optional[WhereClause] = None
    having_clause: Optional[HavingClause] = None
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Optional[Metadata] = field(default_factory=Metadata)
    local_concepts: EnvironmentConceptDict = field(
        default_factory=EnvironmentConceptDict
    )
    derive: DeriveClause | None = None

    def as_lineage(self, environment: Environment):
        return MultiSelectLineage(
            selects=[x.as_lineage(environment) for x in self.selects],
            align=self.align,
            derive=self.derive,
            namespace=self.namespace,
            # derived_concepts = self.derived_concepts,
            limit=self.limit,
            order_by=self.order_by,
            where_clause=self.where_clause,
            having_clause=self.having_clause,
            hidden_components=self.hidden_components,
        )

    def __repr__(self):
        return "MultiSelect<" + " MERGE ".join([str(s) for s in self.selects]) + ">"

    @property
    def grain(self):
        base = Grain()
        for select in self.selects:
            base += select.grain
        return base

    @property
    def output_components(self) -> List[ConceptRef]:
        output = [x.reference for x in self.derived_concepts]
        for select in self.selects:
            output += [
                x
                for x in select.output_components
                if x.address not in select.hidden_components
            ]
        return unique(output, "address")

    @cached_property
    def hidden_components(self) -> set[str]:
        output: set[str] = set()
        for select in self.selects:
            output = output.union(select.hidden_components)
        return output

    @property
    def locally_derived(self) -> set[str]:
        locally_derived: set[str] = set([x.address for x in self.derived_concepts])
        for select in self.selects:
            locally_derived = locally_derived.union(select.locally_derived)
        return locally_derived


@dataclass
class RowsetDerivationStatement(HasUUID):
    name: str
    select: SelectStatement | MultiSelectStatement
    namespace: str

    def __repr__(self):
        return f"RowsetDerivation<{str(self.select)}>"

    def __str__(self):
        return self.__repr__()


@dataclass
class MergeStatementV2(HasUUID):
    sources: list[Concept]
    targets: dict[str, Concept]
    source_wildcard: str | None = None
    target_wildcard: str | None = None
    modifiers: List[Modifier] = field(default_factory=list)


@dataclass
class KeyMergeStatement(HasUUID):
    keys: set[str]
    target: ConceptRef


@dataclass
class ImportStatement(HasUUID):
    # import abc.def as bar
    # the bit after 'as', eg bar
    alias: str
    # the bit after import, abc.def
    input_path: str
    # what it actually resolves to, typically a filepath
    path: Path
    # whether this is a self-import (self import as X)
    is_self: bool = False
    # explicit concept filter: import field1, field2 from abc
    concepts: list[str] | None = None


@dataclass
class PersistStatement(HasUUID):
    datasource: Datasource
    select: SelectStatement
    persist_mode: PersistMode = PersistMode.OVERWRITE
    partition_by: List[ConceptRef] = field(default_factory=list)
    meta: Optional[Metadata] = field(default_factory=Metadata)

    @property
    def identifier(self):
        return self.datasource.identifier

    @property
    def address(self):
        return self.datasource.address


@dataclass
class ValidateStatement:
    scope: ValidationScope
    targets: list[str] | None = None


@dataclass
class MockStatement:
    scope: ValidationScope
    targets: list[str]


@dataclass
class PublishStatement:
    scope: ValidationScope
    targets: list[str]
    action: PublishAction = PublishAction.PUBLISH


@dataclass
class CreateStatement:
    scope: ValidationScope
    create_mode: CreateMode = CreateMode.CREATE_OR_REPLACE
    targets: list[str] = field(default_factory=list)


@dataclass
class ShowStatement:
    content: SelectStatement | PersistStatement | ValidateStatement | ShowCategory


@dataclass
class Limit:
    count: int


@dataclass
class ConceptDeclarationStatement(HasUUID):
    concept: Concept


@dataclass
class PropertiesDeclarationStatement(HasUUID):
    concepts: list[Concept]


@dataclass
class ConceptDerivationStatement:
    concept: Concept


@dataclass
class TypeDeclaration:
    type: CustomType


@dataclass
class FunctionDeclaration(HasUUID):
    name: str
    args: list[ArgBinding]
    expr: Expr
    meta: Optional[Metadata] = field(default_factory=Metadata)


@dataclass
class ChartConfig:
    chart_type: ChartType
    x_fields: list[str] = field(default_factory=list)
    y_fields: list[str] = field(default_factory=list)
    color_field: str | None = None
    size_field: str | None = None
    group_field: str | None = None
    trellis_field: str | None = None
    trellis_row_field: str | None = None
    geo_field: str | None = None
    annotation_field: str | None = None
    hide_legend: bool = False
    show_title: bool = False
    scale_x: Literal["linear", "log", "sqrt"] | None = None
    scale_y: Literal["linear", "log", "sqrt"] | None = None


@dataclass
class ChartStatement:
    config: ChartConfig
    select: SelectStatement
    meta: Optional[Metadata] = field(default_factory=Metadata)


STATEMENT_TYPES = (
    SelectStatement
    | RawSQLStatement
    | CopyStatement
    | MultiSelectStatement
    | RowsetDerivationStatement
    | MergeStatementV2
    | KeyMergeStatement
    | ImportStatement
    | PersistStatement
    | ValidateStatement
    | PublishStatement
    | CreateStatement
    | ShowStatement
    | ConceptDeclarationStatement
    | ConceptDerivationStatement
    | TypeDeclaration
    | FunctionDeclaration
    | ChartStatement
)
