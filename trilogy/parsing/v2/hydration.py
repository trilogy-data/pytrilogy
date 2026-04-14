from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from trilogy.constants import Parsing
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import Comment
from trilogy.core.models.environment import Environment, Import
from trilogy.core.statements.author import ConceptDeclarationStatement
from trilogy.parsing.helpers import comment_body
from trilogy.parsing.v2.import_service import ImportHydrationService
from trilogy.parsing.v2.rules.concept_rules import CONCEPT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.conditional_rules import CONDITIONAL_NODE_HYDRATORS
from trilogy.parsing.v2.rules.expression_rules import EXPRESSION_NODE_HYDRATORS
from trilogy.parsing.v2.rules.function_rules import FUNCTION_NODE_HYDRATORS
from trilogy.parsing.v2.rules.import_rules import IMPORT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.select_rules import SELECT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.statement_rules import STATEMENT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.token_rules import TOKEN_HYDRATORS
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.semantic_scope import SymbolTable
from trilogy.parsing.v2.semantic_state import SemanticState
from trilogy.parsing.v2.statement_planner import (
    StatementPlanner,
    require_block_statement,
)
from trilogy.parsing.v2.statement_plans import (
    ConceptStatementPlan,
    StatementPlan,
    StatementPlanBase,
    UnsupportedSyntaxError,
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
    # LOAD_IMPORTS runs before COLLECT_SYMBOLS because later statements
    # need imported concepts/functions/datasources available during
    # symbol collection and binding. Only ImportStatementPlan does work
    # here; every other plan no-ops.
    LOAD_IMPORTS = "load_imports"
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
    parsed_environments: dict[str, Environment] | None = None
    text_lookup: dict[Path | str, str] | None = None
    import_keys: list[str] | None = None
    symbol_table: SymbolTable | None = None
    semantic_state: SemanticState | None = None
    in_flight_imports: set[str] | None = None


class NativeHydrator:
    def __init__(self, context: HydrationContext) -> None:
        self.environment = context.environment
        self.parse_address = context.parse_address
        self.token_address = context.token_address
        self.semantic_state: SemanticState = (
            context.semantic_state
            if context.semantic_state is not None
            else SemanticState(environment=self.environment)
        )
        self.import_service = ImportHydrationService(
            environment=context.environment,
            parse_config=context.parse_config,
            max_parse_depth=context.max_parse_depth,
            parsed_environments=(
                context.parsed_environments
                if context.parsed_environments is not None
                else {}
            ),
            text_lookup=context.text_lookup if context.text_lookup is not None else {},
            import_keys=list(context.import_keys) if context.import_keys else [],
            in_flight_imports=(
                context.in_flight_imports
                if context.in_flight_imports is not None
                else set()
            ),
            semantic_state=self.semantic_state,
        )
        self.function_factory = FunctionFactory(self.environment)
        self.symbol_table: SymbolTable = (
            context.symbol_table
            if context.symbol_table is not None
            else SymbolTable(self.environment)
        )
        self.plans: list[StatementPlan] = []
        self._planner = StatementPlanner()
        self._cached_rule_context: RuleContext = RuleContext(
            environment=self.environment,
            function_factory=self.function_factory,
            symbol_table=self.symbol_table,
            semantic_state=self.semantic_state,
            source_text="",
        )

    @property
    def parse_config(self) -> Parsing | None:
        return self.import_service.parse_config

    @parse_config.setter
    def parse_config(self, value: Parsing | None) -> None:
        self.import_service.parse_config = value

    @property
    def max_parse_depth(self) -> int:
        return self.import_service.max_parse_depth

    @max_parse_depth.setter
    def max_parse_depth(self, value: int) -> None:
        self.import_service.max_parse_depth = value

    @property
    def parsed_environments(self) -> dict[str, Environment]:
        return self.import_service.parsed_environments

    @parsed_environments.setter
    def parsed_environments(self, value: dict[str, Environment]) -> None:
        self.import_service.parsed_environments = value

    @property
    def text_lookup(self) -> dict[Path | str, str]:
        return self.import_service.text_lookup

    @text_lookup.setter
    def text_lookup(self, value: dict[Path | str, str]) -> None:
        self.import_service.text_lookup = value

    @property
    def import_keys(self) -> list[str]:
        return self.import_service.import_keys

    @import_keys.setter
    def import_keys(self, value: list[str]) -> None:
        self.import_service.import_keys = value

    def set_text(self, text: str) -> None:
        self.import_service.set_text(self.token_address, text)

    def parse(self, document: SyntaxDocument) -> list[Any]:
        self.set_text(document.text)
        self._cached_rule_context = RuleContext(
            environment=self.environment,
            function_factory=self.function_factory,
            symbol_table=self.symbol_table,
            semantic_state=self.semantic_state,
            source_text=self.text_lookup.get(self.token_address, ""),
        )
        try:
            self.plans = self.plan(document.forms)
            # LOAD_IMPORTS is the explicit early import materialization phase
            # and intentionally stays outside the rollback window: imports
            # mutate the environment via add_import and should persist across
            # parse failures in later statements.
            self._run_phase(HydrationPhase.LOAD_IMPORTS)
            self._run_phase(HydrationPhase.COLLECT_SYMBOLS)
            self._run_phase(HydrationPhase.BIND)
            # Interleave hydrate/validate/commit per plan so commit-side env
            # mutations in plan N are visible to hydrate in plan N+1.
            # pending_overlay_scope installs a read-only overlay on the
            # env concept dict so v1 helpers called during hydrate/validate
            # see pending concepts. Commit-time writes that flush pending
            # state (``semantic_state.commit``) temporarily drop the overlay
            # so merge_concept walks durable env.concepts.
            output: list[Any] = []
            with self.semantic_state.pending_overlay_scope():
                for plan in self.plans:
                    plan.hydrate(self)
                    plan.validate(self)
                    output.append(plan.commit(self))
        except BaseException:
            self.semantic_state.rollback()
            raise
        self.semantic_state.commit(self.environment)
        self._resolve_pending_self_imports()
        return [item for item in output if item]

    def _resolve_pending_self_imports(self) -> None:
        """Materialize `self import as X` aliases after the current parse commits.

        Mirrors v1's ``_resolve_pending_self_imports`` phase. Runs after the
        final ``semantic_state.commit`` so ``environment.concepts`` and
        ``environment.datasources`` contain every declaration from this parse;
        ``environment.add_import`` then copies them under the self-import
        alias, replacing any deferred placeholder that was created while
        hydrating references like ``parent.id``.
        """
        pending = self.semantic_state.drain_pending_self_imports()
        if not pending:
            return
        for alias, path in pending:
            import_path = path if path is not None else Path(".")
            self.environment.add_import(
                alias,
                self.environment,
                Import(alias=alias, path=import_path, input_path=path),
            )

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
        # Concept hydration calls v1 function helpers (FunctionFactory /
        # parsing.common) which read the environment's concept dict
        # directly. The pending overlay exposes staged concepts to those
        # reads without mutating the underlying store, so forward
        # references resolve during hydrate without any parse-time write.
        with self.semantic_state.pending_overlay_scope():
            for plan in sorted_concepts:
                plan.output = self.hydrate_concept_block(plan.syntax)

    def block_statement(self, block: SyntaxNode) -> SyntaxNode:
        return require_block_statement(block)

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
        concept_node = self.block_statement(block)
        output = self.hydrate_rule(concept_node)
        trailing = block.children[1:]
        # Match v1: a blank line between the concept and the next comment
        # detaches the comment, so it is preserved as a standalone element
        # rather than mutating the concept's description.
        if concept_node.meta is None:
            return output
        base_line = concept_node.meta.end_line
        comments = []
        for x in trailing:
            if (
                isinstance(x, SyntaxToken)
                and x.kind == SyntaxTokenKind.COMMENT
                and x.meta is not None
                and x.meta.line == base_line
            ):
                comments.append(self.hydrate_comment(x))
                base_line = x.meta.end_line
            else:
                break
        if comments:
            output.concept.metadata.description = "\n".join(
                comment_body(comment) for comment in comments
            )
        return output

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
