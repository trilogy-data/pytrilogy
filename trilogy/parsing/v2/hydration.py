from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from trilogy.constants import DEFAULT_NAMESPACE, Parsing
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import (
    Comment,
    Concept,
    Metadata,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    ImportStatement,
    SelectStatement,
    ShowStatement,
)
from trilogy.parsing.v2.concept_rules import (
    CONCEPT_NODE_HYDRATORS,
    parse_concept_reference,
)
from trilogy.parsing.v2.concept_syntax import (
    ConceptDeclarationSyntax,
    ConceptDerivationSyntax,
    ConceptPropertyDeclarationSyntax,
    ConstantDerivationSyntax,
    PropertyIdentifierSyntax,
)
from trilogy.parsing.v2.conditional_rules import CONDITIONAL_NODE_HYDRATORS
from trilogy.parsing.v2.expression_rules import EXPRESSION_NODE_HYDRATORS
from trilogy.parsing.v2.function_rules import FUNCTION_NODE_HYDRATORS
from trilogy.parsing.v2.import_rules import (
    IMPORT_NODE_HYDRATORS,
    import_statement,
    selective_import_statement,
    self_import_statement,
)
from trilogy.parsing.v2.model import HydrationDiagnostic, RecordingEnvironmentUpdate
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.select_rules import SELECT_NODE_HYDRATORS
from trilogy.parsing.v2.statement_rules import STATEMENT_NODE_HYDRATORS
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
NODE_HYDRATORS = (
    CONCEPT_NODE_HYDRATORS
    | EXPRESSION_NODE_HYDRATORS
    | CONDITIONAL_NODE_HYDRATORS
    | SELECT_NODE_HYDRATORS
    | IMPORT_NODE_HYDRATORS
    | FUNCTION_NODE_HYDRATORS
    | STATEMENT_NODE_HYDRATORS
)

# Concept kinds that define new concepts (used in collect_symbols)
_CONCEPT_INNER_KINDS = {
    SyntaxNodeKind.CONCEPT_DECLARATION,
    SyntaxNodeKind.CONCEPT_DERIVATION,
    SyntaxNodeKind.CONSTANT_DERIVATION,
    SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION,
    SyntaxNodeKind.PARAMETER_DECLARATION,
    SyntaxNodeKind.PROPERTIES_DECLARATION,
}


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


def find_concept_literals(element: SyntaxElement) -> list[SyntaxNode]:
    """Walk a syntax tree and return all CONCEPT_LITERAL nodes."""
    result: list[SyntaxNode] = []
    stack: list[SyntaxElement] = [element]
    while stack:
        node = stack.pop()
        if isinstance(node, SyntaxNode):
            if node.kind == SyntaxNodeKind.CONCEPT_LITERAL:
                result.append(node)
            else:
                stack.extend(node.children)
    return result


def extract_concept_name_from_literal(node: SyntaxNode, namespace: str) -> str:
    """Extract the fully-qualified concept address from a CONCEPT_LITERAL node."""
    token = node.children[0]
    assert isinstance(token, SyntaxToken)
    name = token.value
    if "." not in name and namespace == DEFAULT_NAMESPACE:
        name = f"{DEFAULT_NAMESPACE}.{name}"
    return name


def _get_concept_inner_node(block: SyntaxNode) -> SyntaxNode:
    """Get the inner concept node (declaration/derivation/etc) from a BLOCK > CONCEPT."""
    statement = block.children[0]
    assert isinstance(statement, SyntaxNode)
    assert statement.kind == SyntaxNodeKind.CONCEPT
    nodes = [
        c
        for c in statement.children
        if isinstance(c, SyntaxNode) and c.kind in _CONCEPT_INNER_KINDS
    ]
    assert len(nodes) == 1
    return nodes[0]


def _register_skeleton(
    name: str, namespace: str, environment: Environment
) -> str:
    """Register a minimal placeholder concept for forward-reference resolution.

    Uses direct dict assignment to avoid triggering generate_related_concepts.
    The real concept is added during HYDRATE via add_concept, which overwrites this.
    """
    concept = Concept(
        name=name,
        datatype=DataType.UNKNOWN,
        purpose=Purpose.KEY,
        metadata=Metadata(),
        namespace=namespace,
        derivation=Derivation.ROOT,
        granularity=Granularity.MULTI_ROW,
    )
    environment.concepts[concept.address] = concept
    return concept.address


def collect_concept_symbol(
    block: SyntaxNode, environment: Environment
) -> str | None:
    """Register a skeleton concept in the environment during COLLECT_SYMBOLS.

    Returns the concept address, or None for parameter declarations.
    """
    inner = _get_concept_inner_node(block)
    kind = inner.kind

    if kind == SyntaxNodeKind.CONCEPT_DECLARATION:
        syntax = ConceptDeclarationSyntax.from_node(inner)
        name = syntax.name.value
        _, namespace, name, _ = parse_concept_reference(name, environment)
        return _register_skeleton(name, namespace, environment)

    if kind == SyntaxNodeKind.CONCEPT_DERIVATION:
        syntax = ConceptDerivationSyntax.from_node(inner)
        raw_name = syntax.name
        if isinstance(raw_name, SyntaxToken):
            name_str = raw_name.value
            _, namespace, name_str, _ = parse_concept_reference(
                name_str, environment
            )
        elif isinstance(raw_name, SyntaxNode):
            if raw_name.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER:
                pi = PropertyIdentifierSyntax.from_node(raw_name)
                name_str = pi.name.value
                namespace = environment.namespace or DEFAULT_NAMESPACE
            else:
                name_str = raw_name.children[0].value if raw_name.children else "unknown"
                namespace = environment.namespace or DEFAULT_NAMESPACE
        else:
            name_str = str(raw_name)
            namespace = environment.namespace or DEFAULT_NAMESPACE
        return _register_skeleton(name_str, namespace, environment)

    if kind == SyntaxNodeKind.CONSTANT_DERIVATION:
        syntax_const = ConstantDerivationSyntax.from_node(inner)
        name_str = syntax_const.name.value
        _, namespace, name_str, _ = parse_concept_reference(name_str, environment)
        return _register_skeleton(name_str, namespace, environment)

    if kind == SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION:
        syntax_prop = ConceptPropertyDeclarationSyntax.from_node(inner)
        decl = syntax_prop.declaration
        if isinstance(decl, SyntaxNode) and decl.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER:
            pi = PropertyIdentifierSyntax.from_node(decl)
            name_str = pi.name.value
        elif isinstance(decl, SyntaxToken):
            raw = decl.value
            # "parent.name" format — extract just the property name
            name_str = raw.rsplit(".", 1)[-1] if "." in raw else raw
        else:
            tokens = [
                c
                for c in (decl.children if isinstance(decl, SyntaxNode) else [])
                if isinstance(c, SyntaxToken)
            ]
            name_str = tokens[-1].value if tokens else "unknown"
        namespace = environment.namespace or DEFAULT_NAMESPACE
        return _register_skeleton(name_str, namespace, environment)

    # PARAMETER_DECLARATION, PROPERTIES_DECLARATION — not tracked for ordering
    # Properties declarations create multiple concepts; they hydrate in-place.
    return None


def extract_dependencies(
    block: SyntaxNode, environment: Environment
) -> list[str]:
    """Find all concept addresses referenced in a concept block's source expression."""
    inner = _get_concept_inner_node(block)
    kind = inner.kind
    namespace = environment.namespace or DEFAULT_NAMESPACE

    if kind == SyntaxNodeKind.CONCEPT_DERIVATION:
        syntax = ConceptDerivationSyntax.from_node(inner)
        literals = find_concept_literals(syntax.source)
        # Also check for property grain refs in the name
        if isinstance(syntax.name, SyntaxNode):
            literals.extend(find_concept_literals(syntax.name))
    elif kind == SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION:
        syntax_prop = ConceptPropertyDeclarationSyntax.from_node(inner)
        literals = find_concept_literals(syntax_prop.declaration)
    elif kind == SyntaxNodeKind.CONSTANT_DERIVATION:
        syntax_const = ConstantDerivationSyntax.from_node(inner)
        literals = find_concept_literals(syntax_const.source)
    else:
        return []

    return [extract_concept_name_from_literal(lit, namespace) for lit in literals]


def topological_sort_plans(
    concept_plans: list["ConceptStatementPlan"],
    environment: Environment,
) -> list["ConceptStatementPlan"]:
    """Sort concept plans so dependencies are hydrated first."""
    if not concept_plans:
        return []

    addr_to_plan: dict[str | None, "ConceptStatementPlan"] = {}
    for plan in concept_plans:
        addr_to_plan[plan.address] = plan

    # Build adjacency: address -> set of addresses it depends on
    graph: dict[str | None, list[str]] = {}
    for plan in concept_plans:
        if plan.address is not None:
            deps = [
                d for d in plan.dependencies
                if d in addr_to_plan and d != plan.address
            ]
            graph[plan.address] = deps

    # Kahn's algorithm
    in_degree: dict[str | None, int] = defaultdict(int)
    for addr in graph:
        in_degree.setdefault(addr, 0)
        for dep in graph[addr]:
            in_degree[dep] = in_degree.get(dep, 0) + 1

    # Wait — reverse: if A depends on B, B must come first.
    # So edges should be: B -> A (B blocks A).
    # Let's redo: graph[addr] = things addr depends on.
    # For topo sort: in_degree counts how many things depend on you (not helpful).
    # Standard approach: edges from dependency to dependent.
    forward: dict[str | None, list[str | None]] = defaultdict(list)
    in_deg: dict[str | None, int] = {}
    for addr in graph:
        in_deg[addr] = len(graph[addr])
        for dep in graph[addr]:
            forward[dep].append(addr)

    queue: deque[str | None] = deque()
    for addr in graph:
        if in_deg.get(addr, 0) == 0:
            queue.append(addr)

    ordered: list[str | None] = []
    while queue:
        addr = queue.popleft()
        ordered.append(addr)
        for dependent in forward.get(addr, []):
            in_deg[dependent] -= 1
            if in_deg[dependent] == 0:
                queue.append(dependent)

    # Plans without addresses (parameter declarations) go first
    result: list["ConceptStatementPlan"] = [
        p for p in concept_plans if p.address is None
    ]
    for addr in ordered:
        result.append(addr_to_plan[addr])

    # Any remaining (cycles) — append in original order
    seen = set(id(p) for p in result)
    for plan in concept_plans:
        if id(plan) not in seen:
            result.append(plan)

    return result


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
    address: str | None = None
    dependencies: list[str] = field(default_factory=list)

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        self.address = collect_concept_symbol(
            self.syntax, hydrator.environment
        )

    def bind(self, hydrator: "NativeHydrator") -> None:
        self.dependencies = extract_dependencies(
            self.syntax, hydrator.environment
        )

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
class ImportStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: ImportStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        kind = self.syntax.kind
        if kind == SyntaxNodeKind.IMPORT_STATEMENT:
            self.output = import_statement(
                self.syntax,
                hydrator.rule_context(),
                hydrator.hydrate_rule,
                hydrator=hydrator,
            )
        elif kind == SyntaxNodeKind.SELECTIVE_IMPORT_STATEMENT:
            self.output = selective_import_statement(
                self.syntax,
                hydrator.rule_context(),
                hydrator.hydrate_rule,
                hydrator=hydrator,
            )
        elif kind == SyntaxNodeKind.SELF_IMPORT_STATEMENT:
            self.output = self_import_statement(
                self.syntax,
                hydrator.rule_context(),
                hydrator.hydrate_rule,
                hydrator=hydrator,
            )

    def commit(self, hydrator: "NativeHydrator") -> ImportStatement | None:
        return self.output


@dataclass
class SelectStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: SelectStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> SelectStatement | None:
        return self.output


@dataclass
class GenericStatementPlan(StatementPlanBase):
    """Handles any statement type registered in NODE_HYDRATORS."""

    syntax: SyntaxNode
    output: Any = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> Any:
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
        plans = self.plan(document.forms)
        self._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        self._run_phase(plans, HydrationPhase.BIND)
        # Reorder concept plans by dependency graph
        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        if concept_plans:
            sorted_concepts = topological_sort_plans(
                concept_plans, self.environment
            )
            # Rebuild plan list preserving non-concept order
            concept_iter = iter(sorted_concepts)
            plans = [
                next(concept_iter) if isinstance(p, ConceptStatementPlan) else p
                for p in plans
            ]
        # Allow undefined concept references during hydration for inline
        # derivations in SELECT statements (e.g. `sum(x) -> new_name`).
        self.environment.concepts.fail_on_missing = False
        try:
            self._run_phase(plans, HydrationPhase.HYDRATE)
            self._run_phase(plans, HydrationPhase.VALIDATE)
            output = self._run_phase(plans, HydrationPhase.COMMIT)
            return [item for item in output if item]
        finally:
            self.environment.concepts.fail_on_missing = True

    def plan(self, forms: list[SyntaxElement]) -> list[StatementPlan]:
        return [self.plan_form(form) for form in forms]

    _IMPORT_KINDS = {
        SyntaxNodeKind.IMPORT_STATEMENT,
        SyntaxNodeKind.SELECTIVE_IMPORT_STATEMENT,
        SyntaxNodeKind.SELF_IMPORT_STATEMENT,
    }

    def plan_form(self, form: SyntaxElement) -> StatementPlan:
        if isinstance(form, SyntaxToken):
            if form.kind == SyntaxTokenKind.COMMENT:
                return CommentStatementPlan(form)
            return UnsupportedStatementPlan(form)
        if form.kind == SyntaxNodeKind.SHOW_STATEMENT:
            return ShowStatementPlan(form)
        if form.kind == SyntaxNodeKind.SELECT_STATEMENT:
            return SelectStatementPlan(form)
        if form.kind in self._IMPORT_KINDS:
            return ImportStatementPlan(form)
        if form.kind == SyntaxNodeKind.BLOCK:
            statement = self.block_statement(form)
            if statement.kind == SyntaxNodeKind.CONCEPT:
                return ConceptStatementPlan(syntax=form)
            if statement.kind == SyntaxNodeKind.FUNCTION:
                inner = statement.children[0]
                if (
                    isinstance(inner, SyntaxNode)
                    and inner.kind
                    and NODE_HYDRATORS.get(inner.kind)
                ):
                    return GenericStatementPlan(inner)
                return UnsupportedStatementPlan(statement)
            if statement.kind in self._IMPORT_KINDS:
                return ImportStatementPlan(statement)
            if statement.kind == SyntaxNodeKind.SELECT_STATEMENT:
                return SelectStatementPlan(statement)
            if statement.kind and NODE_HYDRATORS.get(statement.kind):
                return GenericStatementPlan(statement)
            return UnsupportedStatementPlan(statement)
        return UnsupportedStatementPlan(form)

    def _run_phase(
        self, plans: list[StatementPlan], phase: HydrationPhase
    ) -> list[Any]:
        output = []
        for plan in plans:
            output.append(getattr(plan, phase.value)(self))
        return output

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
