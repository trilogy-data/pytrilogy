from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, cast

from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import Concept
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.model import (
    HydrationDiagnostic,
    HydrationError,
    RecordingEnvironmentUpdate,
)
from trilogy.parsing.v2.semantic_scope import SymbolTable
from trilogy.parsing.v2.syntax import SyntaxElement, SyntaxMeta, SyntaxNode, SyntaxToken

HydrateFunction = Callable[[SyntaxElement], Any]
NodeHydrator = Callable[[SyntaxNode, "RuleContext", HydrateFunction], Any]
TokenHydrator = Callable[[SyntaxToken, "RuleContext"], Any]


@dataclass(frozen=True)
class RuleContext:
    environment: Environment
    function_factory: FunctionFactory
    symbol_table: SymbolTable
    source_text: str = ""
    update: RecordingEnvironmentUpdate = field(
        default_factory=RecordingEnvironmentUpdate
    )

    def add_concept(
        self,
        concept: Concept,
        meta: Any | None = None,
        force: bool = False,
    ) -> None:
        self.update.add_concept(self.environment, concept, meta, force=force)


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
