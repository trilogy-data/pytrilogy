from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from trilogy.constants import Parsing
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import (
    AggregateWrapper,
    Comment,
    Concept,
    ConceptRef,
    Function,
    Parenthetical,
    WindowItem,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ShowStatement,
)
from trilogy.parsing.v2.concept_rules import CONCEPT_NODE_HYDRATORS
from trilogy.parsing.v2.expression_rules import EXPRESSION_NODE_HYDRATORS
from trilogy.parsing.v2.model import HydrationDiagnostic, RecordingEnvironmentUpdate
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.statements import (
    hydrate_concept_statement,
    hydrate_show_statement,
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

MAX_PARSE_DEPTH = 10
TRANSPARENT_NODES = {
    SyntaxNodeKind.COMPARISON_ROOT,
    SyntaxNodeKind.SUM_CHAIN,
    SyntaxNodeKind.PRODUCT_CHAIN,
    SyntaxNodeKind.ATOM,
}
NODE_HYDRATORS = CONCEPT_NODE_HYDRATORS | EXPRESSION_NODE_HYDRATORS


class HydrationPhase(Enum):
    COLLECT_SYMBOLS = "collect_symbols"
    BIND = "bind"
    HYDRATE = "hydrate"
    VALIDATE = "validate"
    COMMIT = "commit"


class UnsupportedSyntaxError(NotImplementedError):
    def __init__(
        self,
        message: str,
        diagnostic: HydrationDiagnostic | None = None,
    ) -> None:
        self.diagnostic = diagnostic
        super().__init__(message)

    @classmethod
    def from_syntax(
        cls, message: str, syntax: SyntaxElement
    ) -> "UnsupportedSyntaxError":
        return cls(message, HydrationDiagnostic.from_syntax(message, syntax))


@dataclass
class HydrationContext:
    environment: Environment
    parse_address: str = "root"
    token_address: Path | str = "root"
    parse_config: Parsing | None = None
    max_parse_depth: int = MAX_PARSE_DEPTH


class StatementPlan(Protocol):
    def collect_symbols(self, hydrator: "NativeHydrator") -> None: ...

    def bind(self, hydrator: "NativeHydrator") -> None: ...

    def hydrate(self, hydrator: "NativeHydrator") -> None: ...

    def validate(self, hydrator: "NativeHydrator") -> None: ...

    def commit(self, hydrator: "NativeHydrator") -> Any: ...


class StatementPlanBase:
    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        return None

    def bind(self, hydrator: "NativeHydrator") -> None:
        return None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        return None

    def validate(self, hydrator: "NativeHydrator") -> None:
        return None


@dataclass
class CommentStatementPlan(StatementPlanBase):
    syntax: SyntaxToken
    output: Comment | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_comment(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> Comment | None:
        return self.output


@dataclass
class ConceptStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: ConceptDeclarationStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_concept_block(self.syntax)

    def commit(
        self,
        hydrator: "NativeHydrator",
    ) -> ConceptDeclarationStatement | None:
        return self.output


@dataclass
class ShowStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: ShowStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_show_statement(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> ShowStatement | None:
        return self.output


@dataclass
class UnsupportedStatementPlan(StatementPlanBase):
    syntax: SyntaxElement

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        raise UnsupportedSyntaxError.from_syntax(
            f"No v2 statement plan for syntax node '{syntax_name(self.syntax)}'",
            self.syntax,
        )

    def commit(self, hydrator: "NativeHydrator") -> Any:
        return None


def rehydrate_lineage(
    lineage: Any,
    environment: Environment,
    function_factory: FunctionFactory,
) -> Any:
    if isinstance(lineage, Function):
        rehydrated = [
            rehydrate_lineage(x, environment, function_factory)
            for x in lineage.arguments
        ]
        return function_factory.create_function(
            rehydrated,
            operator=lineage.operator,
        )
    if isinstance(lineage, Parenthetical):
        lineage.content = rehydrate_lineage(
            lineage.content, environment, function_factory
        )
        return lineage
    if isinstance(lineage, WindowItem):
        assert isinstance(lineage.content, ConceptRef)
        lineage.content.datatype = environment.concepts[
            lineage.content.address
        ].datatype
        return lineage
    if isinstance(lineage, AggregateWrapper):
        lineage.function = rehydrate_lineage(
            lineage.function, environment, function_factory
        )
        return lineage
    return lineage


def rehydrate_concept_lineage(
    concept: Concept,
    environment: Environment,
    function_factory: FunctionFactory,
) -> Concept:
    concept.lineage = rehydrate_lineage(concept.lineage, environment, function_factory)
    if hasattr(concept.lineage, "output_datatype"):
        concept.datatype = concept.lineage.output_datatype
    return concept


class NativeHydrator:
    def __init__(self, context: HydrationContext) -> None:
        self.environment = context.environment
        self.parse_address = context.parse_address
        self.token_address = context.token_address
        self.parse_config = context.parse_config
        self.max_parse_depth = context.max_parse_depth
        self.text_lookup: dict[Path | str, str] = {}
        self.function_factory = FunctionFactory(self.environment)
        self.update = RecordingEnvironmentUpdate()
        self._cached_rule_context: RuleContext = RuleContext(
            environment=self.environment,
            function_factory=self.function_factory,
            source_text="",
            update=self.update,
        )

    def set_text(self, text: str) -> None:
        self.text_lookup[self.token_address] = text

    def prepare_parse(self) -> None:
        self.environment.concepts.fail_on_missing = False

    def parse(self, document: SyntaxDocument) -> list[Any]:
        self.set_text(document.text)
        self._cached_rule_context = RuleContext(
            environment=self.environment,
            function_factory=self.function_factory,
            source_text=self.text_lookup.get(self.token_address, ""),
            update=self.update,
        )
        self.prepare_parse()
        plans = self.plan(document.forms)
        try:
            self._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
            self._run_phase(plans, HydrationPhase.BIND)
            self._run_phase(plans, HydrationPhase.HYDRATE)
            self._run_phase(plans, HydrationPhase.VALIDATE)
            self.environment.concepts.undefined = {}
            self._rehydrate_unknown_lineages()
            output = self._run_phase(plans, HydrationPhase.COMMIT)
            return [item for item in output if item]
        finally:
            self.environment.concepts.fail_on_missing = True

    def plan(self, forms: list[SyntaxElement]) -> list[StatementPlan]:
        return [self.plan_form(form) for form in forms]

    def plan_form(self, form: SyntaxElement) -> StatementPlan:
        if isinstance(form, SyntaxToken):
            if form.kind == SyntaxTokenKind.COMMENT:
                return CommentStatementPlan(form)
            return UnsupportedStatementPlan(form)
        if form.kind == SyntaxNodeKind.SHOW_STATEMENT:
            return ShowStatementPlan(form)
        if form.kind == SyntaxNodeKind.BLOCK:
            statement = self.block_statement(form)
            if statement.kind == SyntaxNodeKind.CONCEPT:
                return ConceptStatementPlan(form)
            return UnsupportedStatementPlan(statement)
        return UnsupportedStatementPlan(form)

    def _run_phase(
        self, plans: list[StatementPlan], phase: HydrationPhase
    ) -> list[Any]:
        output = []
        for plan in plans:
            output.append(getattr(plan, phase.value)(self))
        return output

    def _rehydrate_unknown_lineages(self) -> None:
        passed = False
        passes = 0
        while not passed:
            new_passed = True
            for key, concept in self.environment.concepts.items():
                if concept.datatype == DataType.UNKNOWN and concept.lineage:
                    self.environment.concepts[key] = rehydrate_concept_lineage(
                        concept,
                        self.environment,
                        self.function_factory,
                    )
                    new_passed = False
            passes += 1
            if passes > self.max_parse_depth:
                break
            passed = new_passed

    def block_statement(self, block: SyntaxNode) -> SyntaxNode:
        self.require_node(block, SyntaxNodeKind.BLOCK)
        statement = block.children[0]
        if not isinstance(statement, SyntaxNode):
            raise UnsupportedSyntaxError.from_syntax(
                f"Expected statement node in block, got token '{syntax_name(statement)}'",
                statement,
            )
        return statement

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
