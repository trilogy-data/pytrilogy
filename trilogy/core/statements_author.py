from functools import cached_property
from pathlib import Path
from typing import Annotated, List, Optional, Union

from pydantic import BaseModel, Field, computed_field, field_validator
from pydantic.functional_validators import PlainValidator

from trilogy.constants import CONFIG, DEFAULT_NAMESPACE
from trilogy.core.enums import (
    ConceptSource,
    FunctionClass,
    IOType,
    Modifier,
    ShowCategory,
)
from trilogy.core.models_author import (
    AggregateWrapper,
    AlignItem,
    Concept,
    ConceptTransform,
    FilterItem,
    Function,
    Grain,
    HasUUID,
    HavingClause,
    Mergeable,
    Metadata,
    Namespaced,
    OrderBy,
    RowsetItem,
    UndefinedConcept,
    WhereClause,
    WindowItem,
)
from trilogy.core.models_datasource import Address, ColumnAssignment, Datasource
from trilogy.core.models_environment import Environment, EnvironmentConceptDict, validate_concepts
from trilogy.core.models_execute import CTE, UnionCTE
from trilogy.core.statements_common import SelectTypeMixin
from trilogy.utility import unique


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


class SelectStatement(HasUUID, Mergeable, Namespaced, SelectTypeMixin, BaseModel):
    selection: List[SelectItem]
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = None
    meta: Metadata = Field(default_factory=lambda: Metadata())
    local_concepts: Annotated[
        EnvironmentConceptDict, PlainValidator(validate_concepts)
    ] = Field(default_factory=EnvironmentConceptDict)
    grain: Grain = Field(default_factory=Grain)

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
        for parse_pass in [
            1,
            2,
        ]:
            # the first pass will result in all concepts being defined
            # the second will get grains appropriately
            # eg if someone does sum(x)->a, b+c -> z - we don't know if Z is a key to group by or an aggregate
            # until after the first pass, and so don't know the grain of a

            if parse_pass == 1:
                grain = Grain.from_concepts(
                    [
                        x.content
                        for x in output.selection
                        if isinstance(x.content, Concept)
                    ],
                    where_clause=output.where_clause,
                )
            if parse_pass == 2:
                grain = Grain.from_concepts(
                    output.output_components, where_clause=output.where_clause
                )
            output.grain = grain
            pass_grain = Grain() if parse_pass == 1 else grain
            for item in selection:
                # we don't know the grain of an aggregate at assignment time
                # so rebuild at this point in the tree
                # TODO: simplify
                if isinstance(item.content, ConceptTransform):
                    new_concept = item.content.output.with_select_context(
                        output.local_concepts,
                        # the first pass grain will be incorrect
                        pass_grain,
                        environment=environment,
                    )
                    output.local_concepts[new_concept.address] = new_concept
                    item.content.output = new_concept
                    if parse_pass == 2 and CONFIG.select_as_definition:
                        environment.add_concept(new_concept)
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
                        output.local_concepts,
                        pass_grain,
                        environment=environment,
                    )
                    output.local_concepts[item.content.address] = item.content

        if order_by:
            output.order_by = order_by.with_select_context(
                local_concepts=output.local_concepts,
                grain=output.grain,
                environment=environment,
            )
        if output.having_clause:
            output.having_clause = output.having_clause.with_select_context(
                local_concepts=output.local_concepts,
                grain=output.grain,
                environment=environment,
            )
        output.validate_syntax(environment)
        return output

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

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "SelectStatement":
        return SelectStatement(
            selection=[x.with_merge(source, target, modifiers) for x in self.selection],
            order_by=(
                self.order_by.with_merge(source, target, modifiers)
                if self.order_by
                else None
            ),
            limit=self.limit,
        )

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


class RawSQLStatement(BaseModel):
    text: str
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())


class CopyStatement(BaseModel):
    target: str
    target_type: IOType
    meta: Optional[Metadata] = Field(default_factory=lambda: Metadata())
    select: SelectStatement


class AlignClause(Namespaced, BaseModel):
    items: List[AlignItem]

    def with_namespace(self, namespace: str) -> "AlignClause":
        return AlignClause(items=[x.with_namespace(namespace) for x in self.items])


class MultiSelectStatement(HasUUID, SelectTypeMixin, Mergeable, Namespaced, BaseModel):
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

    def __repr__(self):
        return "MultiSelect<" + " MERGE ".join([str(s) for s in self.selects]) + ">"

    @property
    def arguments(self) -> List[Concept]:
        output = []
        for select in self.selects:
            output += select.input_components
        return unique(output, "address")

    @property
    def concept_arguments(self) -> List[Concept]:
        output = []
        for select in self.selects:
            output += select.input_components
        if self.where_clause:
            output += self.where_clause.concept_arguments
        return unique(output, "address")

    def with_merge(
        self, source: Concept, target: Concept, modifiers: List[Modifier]
    ) -> "MultiSelectStatement":
        new = MultiSelectStatement(
            selects=[s.with_merge(source, target, modifiers) for s in self.selects],
            align=self.align,
            namespace=self.namespace,
            derived_concepts=[
                x.with_merge(source, target, modifiers) for x in self.derived_concepts
            ],
            order_by=(
                self.order_by.with_merge(source, target, modifiers)
                if self.order_by
                else None
            ),
            limit=self.limit,
            meta=self.meta,
            where_clause=(
                self.where_clause.with_merge(source, target, modifiers)
                if self.where_clause
                else None
            ),
        )
        return new

    def with_namespace(self, namespace: str) -> "MultiSelectStatement":
        return MultiSelectStatement(
            selects=[c.with_namespace(namespace) for c in self.selects],
            align=self.align.with_namespace(namespace),
            namespace=namespace,
            derived_concepts=[
                x.with_namespace(namespace) for x in self.derived_concepts
            ],
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
            output += select.output_components
        return unique(output, "address")

    @computed_field  # type: ignore
    @cached_property
    def hidden_components(self) -> set[str]:
        output: set[str] = set()
        for select in self.selects:
            output = output.union(select.hidden_components)
        return output


class RowsetDerivationStatement(HasUUID, Namespaced, BaseModel):
    name: str
    select: SelectStatement | MultiSelectStatement
    namespace: str

    def __repr__(self):
        return f"RowsetDerivation<{str(self.select)}>"

    def __str__(self):
        return self.__repr__()

    @property
    def derived_concepts(self) -> List[Concept]:
        output: list[Concept] = []
        orig: dict[str, Concept] = {}
        for orig_concept in self.select.output_components:
            name = orig_concept.name
            if isinstance(orig_concept.lineage, FilterItem):
                if orig_concept.lineage.where == self.select.where_clause:
                    name = orig_concept.lineage.content.name

            new_concept = Concept(
                name=name,
                datatype=orig_concept.datatype,
                purpose=orig_concept.purpose,
                lineage=RowsetItem(
                    content=orig_concept, where=self.select.where_clause, rowset=self
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
