from functools import cached_property
from pathlib import Path
from typing import Annotated, List, Optional, Union

from pydantic import BaseModel, Field, computed_field, field_validator
from pydantic.functional_validators import PlainValidator

from trilogy.constants import CONFIG
from trilogy.core.enums import (
    FunctionClass,
    IOType,
    Modifier,
    ShowCategory,
)
from trilogy.core.models.author import (
    AggregateWrapper,
    AlignClause,
    ArgBinding,
    Concept,
    ConceptRef,
    CustomType,
    Expr,
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
from trilogy.core.statements.common import SelectTypeMixin
from trilogy.utility import unique


class ConceptTransform(BaseModel):
    function: Function | FilterItem | WindowItem | AggregateWrapper
    output: Concept  # this has to be a full concept, as it may not exist in environment
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
    content: Union[ConceptTransform, ConceptRef]
    modifiers: List[Modifier] = Field(default_factory=list)

    @field_validator("content", mode="before")
    def parse_content(cls, v):
        if isinstance(v, Concept):
            return v.reference
        return v

    @property
    def concept(self) -> ConceptRef:
        if isinstance(self.content, (ConceptRef)):
            return self.content
        elif isinstance(self.content, Concept):
            return self.content.reference
        return self.content.output.reference

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
        return SelectLineage(
            selection=[
                environment.concepts[x.concept.address].reference
                for x in self.selection
            ],
            order_by=self.order_by,
            limit=self.limit,
            where_clause=self.where_clause,
            having_clause=self.having_clause,
            local_concepts=self.local_concepts,
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
    ) -> "SelectStatement":
        output = SelectStatement(
            selection=selection,
            where_clause=where_clause,
            having_clause=having_clause,
            limit=limit,
            order_by=order_by,
            meta=meta or Metadata(),
        )

        output.grain = output.calculate_grain(environment)

        for x in selection:
            if x.is_undefined and environment.concepts.fail_on_missing:
                environment.concepts.raise_undefined(
                    x.concept.address, meta.line_number if meta else None
                )
            elif isinstance(x.content, ConceptTransform):
                if isinstance(x.content.output, UndefinedConcept):
                    continue
                if (
                    CONFIG.parsing.select_as_definition
                    and not environment.frozen
                    and x.concept.address not in environment.concepts
                ):
                    environment.add_concept(x.content.output)
                x.content.output = x.content.output.set_select_grain(
                    output.grain, environment
                )
                # we might not need this
                output.local_concepts[x.content.output.address] = x.content.output

            elif isinstance(x.content, ConceptRef):
                output.local_concepts[x.content.address] = environment.concepts[
                    x.content.address
                ]
        output.validate_syntax(environment)
        return output

    def calculate_grain(self, environment: Environment | None = None) -> Grain:
        targets = []
        for x in self.selection:
            targets.append(x.concept)
        result = Grain.from_concepts(
            targets, where_clause=self.where_clause, environment=environment
        )
        return result

    def validate_syntax(self, environment: Environment):
        if self.where_clause:
            for x in self.where_clause.concept_arguments:
                if isinstance(x, UndefinedConcept):
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
                alias=c.address.replace(".", "_"),
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
    # import abc.def as bar
    # the bit after 'as', eg bar
    alias: str
    # the bit after import, abc.def
    input_path: str
    # what it actually resolves to, typically a filepath
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


class TypeDeclaration(BaseModel):
    type: CustomType


class FunctionDeclaration(BaseModel):
    name: str
    args: list[ArgBinding]
    expr: Expr
