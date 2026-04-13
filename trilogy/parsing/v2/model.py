from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from trilogy.parsing.exceptions import ParseError
from trilogy.parsing.v2.semantic_state import ConceptUpdate, ConceptUpdateKind
from trilogy.parsing.v2.syntax import SyntaxElement, SyntaxMeta, syntax_name

__all__ = [
    "DiagnosticSeverity",
    "HydrationDiagnostic",
    "HydrationError",
    "ConceptUpdate",
    "ConceptUpdateKind",
]


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


class HydrationError(ParseError):
    def __init__(self, diagnostic: HydrationDiagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.message)
