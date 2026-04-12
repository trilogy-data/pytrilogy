from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from trilogy.core.models.author import Concept
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.syntax import SyntaxElement, SyntaxMeta, syntax_name


class DiagnosticSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class HydrationDiagnostic:
    message: str
    severity: DiagnosticSeverity = DiagnosticSeverity.ERROR
    meta: SyntaxMeta | None = None
    syntax_kind: str | None = None
    raw_name: str | None = None

    @classmethod
    def from_syntax(
        cls,
        message: str,
        syntax: SyntaxElement,
        severity: DiagnosticSeverity = DiagnosticSeverity.ERROR,
    ) -> "HydrationDiagnostic":
        return cls(
            message=message,
            severity=severity,
            meta=syntax.meta,
            syntax_kind=syntax_name(syntax),
            raw_name=syntax.name,
        )


class HydrationError(Exception):
    def __init__(self, diagnostic: HydrationDiagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.message)


@dataclass
class ConceptUpdate:
    concept: Concept
    meta: Any | None = None


@dataclass
class RecordingEnvironmentUpdate:
    """Records environment updates while applying them immediately.

    Current v2 hydration needs declarations to be visible to later statements in
    the same parse. This is deliberately not a transaction yet.

    This is the single sanctioned entry point for v2 hydrators to mutate
    ``environment.concepts``. Direct calls to ``environment.add_concept`` or
    writes to ``environment.concepts.data`` inside v2 rule handlers should be
    replaced with ``RuleContext.add_concept`` so every write is recorded here.
    The one deliberate exception is the SymbolTable compatibility shim, which
    materializes short-lived ``UndefinedConceptFull`` placeholders for scoped
    name resolution.
    """

    concepts: list[ConceptUpdate] = field(default_factory=list)

    def add_concept(
        self,
        environment: Environment,
        concept: Concept,
        meta: Any | None = None,
        force: bool = False,
    ) -> None:
        self.concepts.append(ConceptUpdate(concept=concept, meta=meta))
        environment.add_concept(concept, meta, force=force)
