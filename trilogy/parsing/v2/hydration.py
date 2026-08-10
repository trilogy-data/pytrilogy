from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE, Parsing
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import Comment
from trilogy.core.models.environment import Environment, Import
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    PropertiesDeclarationStatement,
)
from trilogy.parsing.helpers import comment_body
from trilogy.parsing.v2.import_service import (
    ImportEnvCacheKey,
    ImportHydrationService,
)
from trilogy.parsing.v2.rules.concept_rules import CONCEPT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.conditional_rules import CONDITIONAL_NODE_HYDRATORS
from trilogy.parsing.v2.rules.expression_rules import EXPRESSION_NODE_HYDRATORS
from trilogy.parsing.v2.rules.function_rules import FUNCTION_NODE_HYDRATORS
from trilogy.parsing.v2.rules.import_rules import IMPORT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.select_rules import SELECT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.statement_rules import STATEMENT_NODE_HYDRATORS
from trilogy.parsing.v2.rules.token_rules import TOKEN_HYDRATORS
from trilogy.parsing.v2.rules.tvf_rules import TVF_NODE_HYDRATORS
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.semantic_scope import SymbolTable
from trilogy.parsing.v2.semantic_state import SemanticState
from trilogy.parsing.v2.statement_planner import (
    StatementPlanner,
    require_block_statement,
)
from trilogy.parsing.v2.statement_plans import (
    ConceptStatementPlan,
    RowsetStatementPlan,
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
    "ConceptStatementPlan",
    "HydrationContext",
    "HydrationPhase",
    "NativeHydrator",
    "StatementPlan",
    "StatementPlanBase",
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
    | TVF_NODE_HYDRATORS
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
    parsed_environments: dict[ImportEnvCacheKey, Environment] | None = None
    text_lookup: dict[Path | str, str] | None = None
    import_keys: list[str] | None = None
    symbol_table: SymbolTable | None = None
    semantic_state: SemanticState | None = None
    in_flight_imports: set[str] | None = None
    closure_stack: list | None = None
    local_closures: dict | None = None
    in_stdlib: bool = False


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
            closure_stack=(
                context.closure_stack if context.closure_stack is not None else []
            ),
            local_closures=(
                context.local_closures if context.local_closures is not None else {}
            ),
            in_stdlib=context.in_stdlib,
            semantic_state=self.semantic_state,
        )
        self.in_stdlib = context.in_stdlib
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
            in_stdlib=self.in_stdlib,
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
    def parsed_environments(self) -> dict[ImportEnvCacheKey, Environment]:
        return self.import_service.parsed_environments

    @parsed_environments.setter
    def parsed_environments(self, value: dict[ImportEnvCacheKey, Environment]) -> None:
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
            in_stdlib=self.in_stdlib,
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
        if phase == HydrationPhase.COLLECT_SYMBOLS:
            self._resolve_concept_addresses()
        if phase == HydrationPhase.BIND:
            self._sort_and_create_concepts()
        return output

    def _resolve_concept_addresses(self) -> None:
        """Fix every concept address, parents before the children that inherit
        from them, then publish the results to the symbol table.

        A property declared ``<parent_key_path>.<name>`` takes its parent key's
        namespace, so its address is not knowable until the parent's is.
        Resolving in that order lets each property read the parent's real
        namespace — the same lookup ``concept_property_declaration`` performs at
        hydration — instead of predicting one. Whatever a prediction got wrong
        would be declared at an address no concept occupies, and
        ``_scoped_placeholder`` reads any declared symbol as license to
        manufacture a placeholder, so the bogus address would bind a dangling
        concept rather than raise.

        Plans whose parent never resolves keep no address at all; they take one
        from the concept hydration builds (``declare_hydrated_symbols``).
        """
        plans = [p for p in self.plans if isinstance(p, ConceptStatementPlan)]
        resolved: set[str] = set()
        pending = plans
        while pending:
            waiting = []
            for plan in pending:
                if plan.resolve_address(self.environment, resolved):
                    resolved.update(plan.provided_addresses)
                else:
                    waiting.append(plan)
            if len(waiting) == len(pending):
                break
            pending = waiting
        for plan in plans:
            plan.declare_symbols(self)

    def _deferred_concept_plans(
        self, concept_plans: list[ConceptStatementPlan]
    ) -> set[int]:
        """Plan ids for concept declarations that read a rowset output declared
        earlier in the same file (``auto total <- sum(rs.amount);``).

        A rowset's outputs are real concepts only once its statement hydrates,
        which happens after BIND. Building such a declaration here would bind it
        to the UNKNOWN-typed forward placeholder ``RowsetStatementPlan``
        collect_symbols staged, permanently typing the derived concept UNKNOWN.
        These plans build in their own ``hydrate`` instead, in source order.
        """
        namespace = self.environment.namespace or DEFAULT_NAMESPACE
        position = {id(p): i for i, p in enumerate(self.plans)}
        rowset_at: dict[str, int] = {}
        for index, plan in enumerate(self.plans):
            if isinstance(plan, RowsetStatementPlan) and plan.rowset_name:
                for prefix in (
                    f"{plan.rowset_name}.",
                    f"{namespace}.{plan.rowset_name}.",
                ):
                    rowset_at.setdefault(prefix, index)
        if not rowset_at:
            return set()
        deferred: set[int] = set()
        for plan in concept_plans:
            needed = [
                index
                for prefix, index in rowset_at.items()
                if any(dep.startswith(prefix) for dep in plan.dependencies)
            ]
            # Only a declaration written AFTER the rowset can wait for it; one
            # written ahead is a forward reference the BIND-time topological
            # sort already serves as well as anything can.
            if needed and position[id(plan)] > max(needed):
                deferred.add(id(plan))
        # A declaration reading a deferred concept must wait on it too. When it
        # cannot — it is declared ahead of that concept — keep the dependency at
        # BIND instead, so the forward reference still resolves.
        addr_to_plan = {
            addr: plan for plan in concept_plans for addr in plan.provided_addresses
        }
        changed = True
        while changed:
            changed = False
            for plan in concept_plans:
                if id(plan) in deferred:
                    continue
                for dep in plan.dependencies:
                    dep_plan = addr_to_plan.get(dep)
                    if dep_plan is None or id(dep_plan) not in deferred:
                        continue
                    if position[id(plan)] > position[id(dep_plan)]:
                        deferred.add(id(plan))
                    else:
                        deferred.discard(id(dep_plan))
                    changed = True
                    break
        return deferred

    def _sort_and_create_concepts(self) -> None:
        concept_plans = [p for p in self.plans if isinstance(p, ConceptStatementPlan)]
        if not concept_plans:
            return
        deferred = self._deferred_concept_plans(concept_plans)
        for plan in concept_plans:
            plan.deferred = id(plan) in deferred
        sorted_concepts = topological_sort_plans(
            [p for p in concept_plans if not p.deferred], self.environment
        )
        concept_iter = iter(sorted_concepts)
        self.plans = [
            (
                p
                if not isinstance(p, ConceptStatementPlan) or p.deferred
                else next(concept_iter)
            )
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
                plan.declare_hydrated_symbols(self)

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
    ) -> ConceptDeclarationStatement | PropertiesDeclarationStatement:
        concept_node = self.block_statement(block)
        output = self.hydrate_rule(concept_node)
        description = self.trailing_description(block, concept_node)
        if description is not None:
            if isinstance(output, PropertiesDeclarationStatement):
                # Attach to the first property — the grouped block has no
                # single description target, so this matches v1 behavior.
                output.concepts[0].metadata.description = description
            else:
                output.concept.metadata.description = description
        return output

    def trailing_description(
        self,
        block: SyntaxNode,
        statement: SyntaxNode,
    ) -> str | None:
        # Match v1: a blank line between the statement and the next comment
        # detaches the comment, so it is preserved as a standalone element
        # rather than mutating the statement's description.
        base_line = statement.end_line
        if base_line is None:
            return None
        comments = []
        for x in block.children[1:]:
            if (
                isinstance(x, SyntaxToken)
                and x.kind == SyntaxTokenKind.COMMENT
                and x.line == base_line
            ):
                comments.append(self.hydrate_comment(x))
                base_line = x.end_line
            else:
                break
        if not comments:
            return None
        return "\n".join(comment_body(c) for c in comments)

    def rule_context(self) -> RuleContext:
        return self._cached_rule_context

    def hydrate_rule(self, element: SyntaxElement) -> Any:
        # type() is faster than isinstance for the concrete dataclasses.
        if type(element) is SyntaxToken:
            return self.hydrate_token(element)
        node: SyntaxNode = element  # type: ignore[assignment]
        kind = node.kind
        handler = NODE_HYDRATORS.get(kind) if kind is not None else None
        if handler is not None:
            return handler(node, self._cached_rule_context, self.hydrate_rule)
        if kind in TRANSPARENT_NODES and len(node.children) == 1:
            return self.hydrate_rule(node.children[0])
        raise UnsupportedSyntaxError.from_syntax(
            f"No v2 hydrator for syntax node '{syntax_name(node)}'",
            node,
        )

    def hydrate_token(self, token: SyntaxToken) -> Any:
        kind = token.kind
        handler = TOKEN_HYDRATORS.get(kind) if kind is not None else None
        if handler is not None:
            return handler(token, self._cached_rule_context)
        return token.value
