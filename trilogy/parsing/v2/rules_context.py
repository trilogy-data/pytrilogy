from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, cast

from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import Concept
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.model import HydrationDiagnostic, HydrationError
from trilogy.parsing.v2.semantic_scope import SymbolTable
from trilogy.parsing.v2.semantic_state import ConceptUpdateKind, SemanticState
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

    def _add_concept(
        self,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
        force: bool = False,
    ) -> None:
        self.semantic_state.add(concept, kind, meta=meta, force=force)

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
