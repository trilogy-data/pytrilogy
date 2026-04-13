from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, cast

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import ConceptSource, FunctionType, Purpose
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import Concept, CustomFunctionFactory, Metadata
from trilogy.core.models.core import StructType, arg_to_datatype
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError
from trilogy.parsing.v2.semantic_scope import SymbolTable
from trilogy.parsing.v2.semantic_state import (
    ConceptLookup,
    ConceptUpdateKind,
    SemanticState,
    TypeLookup,
)
from trilogy.parsing.v2.syntax import SyntaxElement, SyntaxMeta, SyntaxNode, SyntaxToken

HydrateFunction = Callable[[SyntaxElement], Any]
NodeHydrator = Callable[[SyntaxNode, "RuleContext", HydrateFunction], Any]
TokenHydrator = Callable[[SyntaxToken, "RuleContext"], Any]


@dataclass(frozen=True)
class RuleContext:
    environment: Environment
    function_factory: FunctionFactory
    symbol_table: SymbolTable
    semantic_state: SemanticState
    source_text: str = ""
    concepts: ConceptLookup = field(init=False, repr=False, compare=False)
    types: TypeLookup = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "concepts",
            ConceptLookup(self.semantic_state, symbol_table=self.symbol_table),
        )
        object.__setattr__(self, "types", TypeLookup(self.semantic_state))

    def _add_concept(
        self,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
        force: bool = False,
    ) -> None:
        self.semantic_state.add(concept, kind, meta=meta, force=force)
        self._stage_struct_fields(concept, meta=meta)

    def _stage_struct_fields(self, concept: Concept, meta: Any | None = None) -> None:
        """Stage pending-only field concepts for struct-typed concepts.

        Mirrors ``generate_related_concepts``' struct branch so field concepts
        like ``wrapper.a`` resolve through ``ConceptLookup`` during per-plan
        validate. The staged entries are committed as
        ``STRUCT_FIELD_VIRTUAL`` and skipped by ``SemanticState.commit`` — at
        final commit, ``environment.add_concept`` on the parent regenerates
        each field with pseudonyms already merged.
        """
        datatype = concept.datatype
        if not isinstance(datatype, StructType):
            return
        env_namespace = self.environment.namespace
        if env_namespace and env_namespace != DEFAULT_NAMESPACE:
            field_namespace = f"{env_namespace}.{concept.name}"
        else:
            field_namespace = concept.name
        parent_ref = concept.reference
        for field_name, value in datatype.fields_map.items():
            field_address = f"{field_namespace}.{field_name}"
            if isinstance(value, Concept):
                # Mirror ``merge_concept``: expose the existing canonical
                # concept (e.g. ``local.a``) at the field address so select
                # resolution canonicalizes output column names.
                self.semantic_state.stage_field_alias(field_address, value)
                continue
            field_concept = Concept(
                name=field_name,
                datatype=arg_to_datatype(value),
                purpose=Purpose.PROPERTY,
                namespace=field_namespace,
                lineage=self.function_factory.create_function(
                    [parent_ref, field_name], FunctionType.ATTR_ACCESS
                ),
                grain=concept.grain,
                metadata=Metadata(concept_source=ConceptSource.AUTO_DERIVED),
                keys=concept.keys,
            )
            self.semantic_state.add(
                field_concept,
                ConceptUpdateKind.STRUCT_FIELD_VIRTUAL,
                meta=meta,
            )

    def add_top_level_concept(self, concept: Concept, meta: Any | None = None) -> None:
        self._add_concept(concept, ConceptUpdateKind.TOP_LEVEL_DECLARATION, meta=meta)

    def add_property_concept(self, concept: Concept, meta: Any | None = None) -> None:
        self._add_concept(concept, ConceptUpdateKind.PROPERTY_DECLARATION, meta=meta)

    def add_datasource_property_concept(
        self, concept: Concept, meta: Any | None = None
    ) -> None:
        self._add_concept(concept, ConceptUpdateKind.DATASOURCE_PROPERTY, meta=meta)

    def add_select_concept(self, concept: Concept, meta: Any | None = None) -> None:
        self._add_concept(concept, ConceptUpdateKind.SELECT_LOCAL, meta=meta)

    def add_multiselect_concept(
        self, concept: Concept, meta: Any | None = None
    ) -> None:
        self._add_concept(concept, ConceptUpdateKind.MULTISELECT_OUTPUT, meta=meta)

    def add_rowset_concept(
        self,
        concept: Concept,
        meta: Any | None = None,
        force: bool = False,
    ) -> None:
        self._add_concept(
            concept, ConceptUpdateKind.ROWSET_OUTPUT, meta=meta, force=force
        )

    def add_virtual_concept(self, concept: Concept, meta: Any | None = None) -> None:
        self._add_concept(concept, ConceptUpdateKind.VIRTUAL_HELPER, meta=meta)

    @property
    def functions(self) -> dict[str, CustomFunctionFactory]:
        return self.environment.functions


def core_meta(meta: SyntaxMeta | None) -> Any:
    return cast(Any, meta)


def fail(node: SyntaxNode, message: str) -> HydrationError:
    return HydrationError(HydrationDiagnostic.from_syntax(message, node))


def hydrated_children(
    node: SyntaxNode,
    hydrate: HydrateFunction,
) -> list[Any]:
    return [hydrate(child) for child in node.children]


def apply_source_location(concept: Concept, meta: SyntaxMeta | None) -> None:
    if not concept.metadata or not meta:
        return
    concept.metadata.line_number = meta.line
    concept.metadata.column = meta.column
    concept.metadata.end_line = meta.end_line
    concept.metadata.end_column = meta.end_column
