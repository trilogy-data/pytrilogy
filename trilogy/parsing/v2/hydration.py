from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from trilogy.constants import Parsing
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import Comment
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ShowStatement,
)
from trilogy.parsing.v2.concept_rules import CONCEPT_NODE_HYDRATORS
from trilogy.parsing.v2.conditional_rules import CONDITIONAL_NODE_HYDRATORS
from trilogy.parsing.v2.expression_rules import EXPRESSION_NODE_HYDRATORS
from trilogy.parsing.v2.function_rules import FUNCTION_NODE_HYDRATORS
from trilogy.parsing.v2.import_rules import IMPORT_NODE_HYDRATORS
from trilogy.parsing.v2.model import RecordingEnvironmentUpdate
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.select_rules import SELECT_NODE_HYDRATORS
from trilogy.parsing.v2.statement_planner import StatementPlanner, require_block_statement
from trilogy.parsing.v2.statement_plans import (
    ConceptStatementPlan,
    StatementPlan,
    StatementPlanBase,
    UnsupportedSyntaxError,
)
from trilogy.parsing.v2.statement_rules import STATEMENT_NODE_HYDRATORS
from trilogy.parsing.v2.statements import (
    hydrate_concept_statement,
    hydrate_show_statement,
)
from trilogy.parsing.v2.symbols import (
    extract_concept_name_from_literal,
    extract_dependencies,
    find_concept_literals,
    topological_sort_plans,
)
from trilogy.parsing.v2.syntax import (
    SyntaxDocument,
    SyntaxElement,
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
    syntax_name,
)
from trilogy.parsing.v2.token_rules import TOKEN_HYDRATORS

__all__ = [
    "MAX_PARSE_DEPTH",
    "HydrationContext",
    "HydrationPhase",
    "NativeHydrator",
    "StatementPlan",
    "StatementPlanBase",
    "ConceptStatementPlan",
    "UnsupportedSyntaxError",
    "extract_concept_name_from_literal",
    "extract_dependencies",
    "find_concept_literals",
    "topological_sort_plans",
]

MAX_PARSE_DEPTH = 10
TRANSPARENT_NODES = {
    SyntaxNodeKind.COMPARISON_ROOT,
    SyntaxNodeKind.SUM_CHAIN,
    SyntaxNodeKind.PRODUCT_CHAIN,
    SyntaxNodeKind.ATOM,
}
NODE_HYDRATORS = (
    CONCEPT_NODE_HYDRATORS
    | EXPRESSION_NODE_HYDRATORS
    | CONDITIONAL_NODE_HYDRATORS
    | SELECT_NODE_HYDRATORS
    | IMPORT_NODE_HYDRATORS
    | FUNCTION_NODE_HYDRATORS
    | STATEMENT_NODE_HYDRATORS
)


class HydrationPhase(Enum):
    COLLECT_SYMBOLS = "collect_symbols"
    BIND = "bind"
    HYDRATE = "hydrate"
    VALIDATE = "validate"
    COMMIT = "commit"


@dataclass
class HydrationContext:
    environment: Environment
    parse_address: str = "root"
    token_address: Path | str = "root"
    parse_config: Parsing | None = None
    max_parse_depth: int = MAX_PARSE_DEPTH


class NativeHydrator:
    def __init__(self, context: HydrationContext) -> None:
        self.environment = context.environment
        self.parse_address = context.parse_address
        self.token_address = context.token_address
        self.parse_config = context.parse_config
        self.max_parse_depth = context.max_parse_depth
        self.import_keys: list[str] = []
        self.parsed_environments: dict[str, Environment] = {}
        self.text_lookup: dict[Path | str, str] = {}
        self.function_factory = FunctionFactory(self.environment)
        self.update = RecordingEnvironmentUpdate()
        self.plans: list[StatementPlan] = []
        self._planner = StatementPlanner()
        self._cached_rule_context: RuleContext = RuleContext(
            environment=self.environment,
            function_factory=self.function_factory,
            source_text="",
            update=self.update,
        )

    def set_text(self, text: str) -> None:
        self.text_lookup[self.token_address] = text

    def parse(self, document: SyntaxDocument) -> list[Any]:
        self.set_text(document.text)
        self._cached_rule_context = RuleContext(
            environment=self.environment,
            function_factory=self.function_factory,
            source_text=self.text_lookup.get(self.token_address, ""),
            update=self.update,
        )
        self.plans = self.plan(document.forms)
        output: list[Any] = []
        for phase in HydrationPhase:
            output = self._run_phase(phase)
        return [item for item in output if item]

    def plan(self, forms: list[SyntaxElement]) -> list[StatementPlan]:
        return self._planner.plan(forms)

    def _run_phase(self, phase: HydrationPhase) -> list[Any]:
        output = []
        for plan in self.plans:
            output.append(getattr(plan, phase.value)(self))
        if phase == HydrationPhase.BIND:
            self._sort_and_create_concepts()
        return output

    def _sort_and_create_concepts(self) -> None:
        concept_plans = [p for p in self.plans if isinstance(p, ConceptStatementPlan)]
        if not concept_plans:
            return
        sorted_concepts = topological_sort_plans(concept_plans, self.environment)
        concept_iter = iter(sorted_concepts)
        self.plans = [
            next(concept_iter) if isinstance(p, ConceptStatementPlan) else p
            for p in self.plans
        ]
        for plan in sorted_concepts:
            plan.output = self.hydrate_concept_block(plan.syntax)

    def block_statement(self, block: SyntaxNode) -> SyntaxNode:
        return require_block_statement(block)

    def require_node(self, element: SyntaxElement, kind: SyntaxNodeKind) -> SyntaxNode:
        if not isinstance(element, SyntaxNode) or element.kind != kind:
            raise UnsupportedSyntaxError.from_syntax(
                f"Expected syntax node '{kind.value}', got '{syntax_name(element)}'",
                element,
            )
        return element

    def hydrate_comment(self, token: SyntaxToken) -> Comment:
        if token.kind != SyntaxTokenKind.COMMENT:
            raise UnsupportedSyntaxError.from_syntax(
                f"Expected comment token, got '{syntax_name(token)}'",
                token,
            )
        return TOKEN_HYDRATORS[SyntaxTokenKind.COMMENT](token, self.rule_context())

    def hydrate_concept_block(
        self,
        block: SyntaxNode,
    ) -> ConceptDeclarationStatement:
        concept_node = self.require_node(
            self.block_statement(block), SyntaxNodeKind.CONCEPT
        )
        output = self.hydrate_concept_statement(concept_node)
        comments = [
            self.hydrate_comment(child)
            for child in block.children[1:]
            if isinstance(child, SyntaxToken) and child.kind == SyntaxTokenKind.COMMENT
        ]
        if comments:
            output.concept.metadata.description = "\n".join(
                comment.text.split("#")[1].rstrip() for comment in comments
            )
        return output

    def hydrate_concept_statement(
        self,
        concept_node: SyntaxNode,
    ) -> ConceptDeclarationStatement:
        self.require_node(concept_node, SyntaxNodeKind.CONCEPT)
        return hydrate_concept_statement(
            concept_node, self.rule_context(), self.hydrate_rule
        )

    def hydrate_show_statement(self, show_node: SyntaxNode) -> ShowStatement:
        self.require_node(show_node, SyntaxNodeKind.SHOW_STATEMENT)
        return hydrate_show_statement(show_node, self.hydrate_rule)

    def rule_context(self) -> RuleContext:
        return self._cached_rule_context

    def hydrate_rule(self, element: SyntaxElement) -> Any:
        if isinstance(element, SyntaxToken):
            return self.hydrate_token(element)
        handler = NODE_HYDRATORS.get(element.kind) if element.kind else None
        if handler:
            return handler(element, self.rule_context(), self.hydrate_rule)
        if len(element.children) == 1 and element.kind in TRANSPARENT_NODES:
            return self.hydrate_rule(element.children[0])
        raise UnsupportedSyntaxError.from_syntax(
            f"No v2 hydrator for syntax node '{syntax_name(element)}'",
            element,
        )

    def hydrate_token(self, token: SyntaxToken) -> Any:
        handler = TOKEN_HYDRATORS.get(token.kind) if token.kind else None
        if handler:
            return handler(token, self.rule_context())
        return token.value
