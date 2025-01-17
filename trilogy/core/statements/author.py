from functools import cached_property
from pathlib import Path
from typing import Annotated, List, Optional, Union

from pydantic import BaseModel, Field, computed_field, field_validator
from pydantic.functional_validators import PlainValidator

from trilogy.constants import CONFIG, DEFAULT_NAMESPACE
from trilogy.core.enums import (
    FunctionClass,
    IOType,
    Modifier,
    ShowCategory,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    Concept,
    FilterItem,
    Function,
    Grain,
    HasUUID,
    HavingClause,
    Metadata,
    MultiSelectLineage,
    OrderBy,
    SelectLineage,
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
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.statements.common import SelectTypeMixin
from trilogy.utility import unique


class ConceptTransform(BaseModel):
    function: Function | FilterItem | WindowItem | AggregateWrapper
    output: Concept
    modifiers: List[Modifier] = Field(default_factory=list)

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


class SelectItem(BaseModel):
    content: Union[Concept, ConceptTransform]
    modifiers: List[Modifier] = Field(default_factory=list)

    @property
    def concept(self):
        return (
            self.content if isinstance(self.content, Concept) else self.content.output
        )

    @property
    def is_undefined(self) -> bool:
        return True if isinstance(self.content, UndefinedConcept) else False


class SelectStatement(HasUUID, SelectTypeMixin, BaseModel):
    selection: List[SelectItem]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    local_concepts: Annotated[
        EnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=EnvironmentConceptDict)
    grain: Grain = Field(default_factory=Grain)

    def as_lineage(self, environment: Environment) -> SelectLineage:
        self.rebuild_for_select(environment)
        return SelectLineage(
            selection=[x.concept for x in self.selection],
            hidden_components=self.hidden_components,
            order_by=self.order_by,
            limit=self.limit,
            meta=self.meta,
            local_concepts=self.local_concepts,
            grain=self.grain,
            having_clause=self.having_clause,
            where_clause=self.where_clause,
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
    ) -> "SelectStatement":
        output = SelectStatement(
            selection=selection,
            where_clause=where_clause,
            having_clause=having_clause,
            limit=limit,
            order_by=order_by,
            meta=meta or Metadata(),
        )
        output.grain = output.calculate_grain()
        for x in selection:
            if x.is_undefined and environment.concepts.fail_on_missing:
                environment.concepts.raise_undefined(
                    x.concept.address, meta.line_number if meta else None
                )
            elif isinstance(x.content, ConceptTransform):
                if (
                    CONFIG.select_as_definition
                    and not environment.frozen
                    and x.concept.address not in environment.concepts
                ):
                    environment.add_concept(x.concept)
                x.content.output = x.content.output.set_select_grain(
                    output.grain, environment
                )
                # we might not need this
                output.local_concepts[x.concept.address] = x.concept

            elif isinstance(x.content, Concept):
                x.content = x.content.set_select_grain(output.grain, environment)
                output.local_concepts[x.content.address] = x.content

        output.validate_syntax(environment)
        return output

    def calculate_grain(self) -> Grain:
        targets = []
        for x in self.selection:
            targets.append(x.concept)
        result = Grain.from_concepts(
            targets,
            where_clause=self.where_clause,
        )
        return result

    def rebuild_for_select(self, environment: Environment):
        for item in self.selection:
            # we don't know the grain of an aggregate at assignment time
            # so rebuild at this point in the tree
            # TODO: simplify
            if isinstance(item.content, ConceptTransform):
                new_concept = item.content.output.with_select_context(
                    self.local_concepts,
                    # the first pass grain will be incorrect
                    self.grain,
                    environment=environment,
                )
                self.local_concepts[new_concept.address] = new_concept
                item.content.output = new_concept
            elif isinstance(item.content, UndefinedConcept):
                environment.concepts.raise_undefined(
                    item.content.address,
                    line_no=item.content.metadata.line_number,
                    file=environment.env_file_path,
                )
            elif isinstance(item.content, Concept):
                # Sometimes cached values here don't have the latest info
                # but we can't just use environment, as it might not have the right grain.
                item.content = item.content.with_select_context(
                    self.local_concepts,
                    self.grain,
                    environment=environment,
                )
                self.local_concepts[item.content.address] = item.content

        if self.order_by:
            self.order_by = self.order_by.with_select_context(
                local_concepts=self.local_concepts,
                grain=self.grain,
                environment=environment,
            )
        if self.having_clause:
            self.having_clause = self.having_clause.with_select_context(
                local_concepts=self.local_concepts,
                grain=self.grain,
                environment=environment,
            )

        return self

    def validate_syntax(self, environment: Environment):
        if self.where_clause:
            for x in self.where_clause.concept_arguments:
                if isinstance(x, UndefinedConcept):
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
                        f"Cannot order by a column that is not in the output projection; {self.meta.line_number}"
                    )

    def __str__(self):
        from trilogy.parsing.render import render_query

        return render_query(self)

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
                locally_derived.add(item.concept.address)
        return locally_derived

    @property
    def output_components(self) -> List[Concept]:
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
                    c.name.replace(".", "_")
                    if c.namespace == DEFAULT_NAMESPACE
                    else c.address.replace(".", "_")
                ),
                concept=c,
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
        for column in columns:
            column.concept = column.concept.with_grain(new_datasource.grain)
        return new_datasource


class RawSQLStatement(BaseModel):
    text: str
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())


class CopyStatement(BaseModel):
    target: str
    target_type: IOType
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())
    select: SelectStatement


class MultiSelectStatement(HasUUID, SelectTypeMixin, BaseModel):
    selects: List[SelectStatement]
    align: AlignClause
    namespace: str
    derived_concepts: List[Concept]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())
    local_concepts: Annotated[
        EnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=EnvironmentConceptDict)

    def as_lineage(self, environment: Environment):
        return MultiSelectLineage(
            selects=[x.as_lineage(environment) for x in self.selects],
            align=self.align,
            namespace=self.namespace,
            # derived_concepts = self.derived_concepts,
            limit=self.limit,
            order_by=self.order_by,
            where_clause=self.where_clause,
            having_clause=self.having_clause,
            local_concepts=self.local_concepts,
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
    def output_components(self) -> List[Concept]:
        output = self.derived_concepts
        for select in self.selects:
            output += [
                x
                for x in select.output_components
                if x.address not in select.hidden_components
            ]
        return unique(output, "address")

    @computed_field  # type: ignore
    @cached_property
    def hidden_components(self) -> set[str]:
        output: set[str] = set()
        for select in self.selects:
            output = output.union(select.hidden_components)
        return output


class RowsetDerivationStatement(HasUUID, BaseModel):
    name: str
    select: SelectStatement | MultiSelectStatement
    namespace: str

    def __repr__(self):
        return f"RowsetDerivation<{str(self.select)}>"

    def __str__(self):
        return self.__repr__()


class MergeStatementV2(HasUUID, BaseModel):
    sources: list[Concept]
    targets: dict[str, Concept]
    source_wildcard: str | None = None
    target_wildcard: str | None = None
    modifiers: List[Modifier] = Field(default_factory=list)


class ImportStatement(HasUUID, BaseModel):
    alias: str
    path: Path


class PersistStatement(HasUUID, BaseModel):
    datasource: Datasource
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


class Limit(BaseModel):
    count: int


class ConceptDeclarationStatement(HasUUID, BaseModel):
    concept: Concept


class ConceptDerivationStatement(BaseModel):
    concept: Concept
