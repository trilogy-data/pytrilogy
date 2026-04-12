from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from trilogy.constants import DEFAULT_NAMESPACE, Parsing
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import (
    Comment,
)
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    ConceptDeclarationStatement,
    FunctionDeclaration,
    ImportStatement,
    MergeStatementV2,
    MultiSelectStatement,
    PersistStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
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
from trilogy.parsing.v2.function_syntax import FunctionDefinitionSyntax
from trilogy.parsing.v2.import_rules import (
    IMPORT_NODE_HYDRATORS,
    import_statement,
    selective_import_statement,
    self_import_statement,
)
from trilogy.parsing.v2.model import (
    HydrationDiagnostic,
    HydrationError,
    RecordingEnvironmentUpdate,
)
from trilogy.parsing.v2.rules_context import RuleContext
from trilogy.parsing.v2.scopes import (
    temporary_function_scope,
    temporary_rowset_scope,
)
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
    if not node.children or not isinstance(node.children[0], SyntaxToken):
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Concept literal requires a leading identifier token", node
            )
        )
    name = node.children[0].value
    if "." not in name and namespace == DEFAULT_NAMESPACE:
        name = f"{DEFAULT_NAMESPACE}.{name}"
    return name


def find_select_transform_targets(element: SyntaxElement) -> list[str]:
    """Find all concept names created by `expr -> name` in SELECT projections."""
    result: list[str] = []
    stack: list[SyntaxElement] = [element]
    while stack:
        node = stack.pop()
        if isinstance(node, SyntaxNode):
            if node.kind == SyntaxNodeKind.SELECT_TRANSFORM:
                # Last child token is the target identifier
                for child in reversed(node.children):
                    if (
                        isinstance(child, SyntaxToken)
                        and child.kind == SyntaxTokenKind.IDENTIFIER
                    ):
                        result.append(child.value)
                        break
            else:
                stack.extend(node.children)
    return result


def collect_inline_concept_addresses(
    element: SyntaxElement, namespace: str
) -> list[str]:
    """Extract addresses of concepts created inline via `-> name` in SELECT statements."""
    names = find_select_transform_targets(element)
    return [_make_address(n, namespace) for n in names]


def _get_concept_inner_node(block: SyntaxNode) -> SyntaxNode:
    """Get the inner concept node (declaration/derivation/etc) from a BLOCK > CONCEPT."""
    if not block.children:
        raise HydrationError(
            HydrationDiagnostic.from_syntax("Concept block is empty", block)
        )
    statement = block.children[0]
    if (
        not isinstance(statement, SyntaxNode)
        or statement.kind != SyntaxNodeKind.CONCEPT
    ):
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Expected CONCEPT node inside concept block", block
            )
        )
    nodes = [
        c
        for c in statement.children
        if isinstance(c, SyntaxNode) and c.kind in _CONCEPT_INNER_KINDS
    ]
    if len(nodes) != 1:
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                f"Concept block expects a single inner declaration, found {len(nodes)}",
                statement,
            )
        )
    return nodes[0]


def _make_address(name: str, namespace: str) -> str:
    return f"{namespace}.{name}"


def collect_concept_address(block: SyntaxNode, environment: Environment) -> str | None:
    """Extract the concept address from a block without modifying the environment.

    Returns the concept address, or None for parameter/properties declarations.
    """
    inner = _get_concept_inner_node(block)
    kind = inner.kind

    if kind == SyntaxNodeKind.CONCEPT_DECLARATION:
        decl_syntax = ConceptDeclarationSyntax.from_node(inner)
        _, namespace, name, _ = parse_concept_reference(
            decl_syntax.name.value, environment
        )
        return _make_address(name, namespace)

    if kind == SyntaxNodeKind.CONCEPT_DERIVATION:
        derivation_syntax = ConceptDerivationSyntax.from_node(inner)
        raw_name = derivation_syntax.name
        if isinstance(raw_name, SyntaxToken):
            _, namespace, name_str, _ = parse_concept_reference(
                raw_name.value, environment
            )
            return _make_address(name_str, namespace)
        if (
            isinstance(raw_name, SyntaxNode)
            and raw_name.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER
        ):
            property_id = PropertyIdentifierSyntax.from_node(raw_name)
            namespace = environment.namespace or DEFAULT_NAMESPACE
            return _make_address(property_id.name.value, namespace)
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Concept derivation name must be an identifier or property identifier",
                raw_name,
            )
        )

    if kind == SyntaxNodeKind.CONSTANT_DERIVATION:
        const_syntax = ConstantDerivationSyntax.from_node(inner)
        _, namespace, name_str, _ = parse_concept_reference(
            const_syntax.name.value, environment
        )
        return _make_address(name_str, namespace)

    if kind == SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION:
        property_syntax = ConceptPropertyDeclarationSyntax.from_node(inner)
        decl = property_syntax.declaration
        namespace = environment.namespace or DEFAULT_NAMESPACE
        if (
            isinstance(decl, SyntaxNode)
            and decl.kind == SyntaxNodeKind.PROPERTY_IDENTIFIER
        ):
            property_id = PropertyIdentifierSyntax.from_node(decl)
            return _make_address(property_id.name.value, namespace)
        if isinstance(decl, SyntaxToken):
            raw = decl.value
            short = raw.rsplit(".", 1)[-1] if "." in raw else raw
            return _make_address(short, namespace)
        raise HydrationError(
            HydrationDiagnostic.from_syntax(
                "Property declaration target must be a property identifier or token",
                decl,
            )
        )

    # PARAMETER_DECLARATION, PROPERTIES_DECLARATION — no single address
    return None


def collect_properties_addresses(
    block: SyntaxNode, environment: Environment
) -> list[str]:
    """Extract all concept addresses from a PROPERTIES_DECLARATION block."""
    inner = _get_concept_inner_node(block)
    if inner.kind != SyntaxNodeKind.PROPERTIES_DECLARATION:
        return []
    namespace = environment.namespace or DEFAULT_NAMESPACE
    result: list[str] = []
    for child in inner.children:
        if (
            isinstance(child, SyntaxNode)
            and child.kind == SyntaxNodeKind.INLINE_PROPERTY_LIST
        ):
            for prop in child.children:
                if (
                    isinstance(prop, SyntaxNode)
                    and prop.kind == SyntaxNodeKind.INLINE_PROPERTY
                ):
                    for token in prop.children:
                        if (
                            isinstance(token, SyntaxToken)
                            and token.kind == SyntaxTokenKind.IDENTIFIER
                        ):
                            result.append(_make_address(token.value, namespace))
                            break
    return result


def extract_dependencies(block: SyntaxNode, environment: Environment) -> list[str]:
    """Find all concept addresses referenced in a concept block's source expression."""
    inner = _get_concept_inner_node(block)
    kind = inner.kind
    namespace = environment.namespace or DEFAULT_NAMESPACE

    if kind == SyntaxNodeKind.CONCEPT_DERIVATION:
        syntax = ConceptDerivationSyntax.from_node(inner)
        literals = find_concept_literals(syntax.source)
        if isinstance(syntax.name, SyntaxNode):
            literals.extend(find_concept_literals(syntax.name))
    elif kind == SyntaxNodeKind.CONCEPT_PROPERTY_DECLARATION:
        syntax_prop = ConceptPropertyDeclarationSyntax.from_node(inner)
        literals = find_concept_literals(syntax_prop.declaration)
    elif kind == SyntaxNodeKind.CONSTANT_DERIVATION:
        syntax_const = ConstantDerivationSyntax.from_node(inner)
        literals = find_concept_literals(syntax_const.source)
    elif kind == SyntaxNodeKind.PROPERTIES_DECLARATION:
        # Properties blocks depend on their grain concepts
        deps: list[str] = []
        for child in inner.children:
            if (
                isinstance(child, SyntaxNode)
                and child.kind == SyntaxNodeKind.PROP_IDENT_LIST
            ):
                for token in child.children:
                    if (
                        isinstance(token, SyntaxToken)
                        and token.kind == SyntaxTokenKind.IDENTIFIER
                    ):
                        name = token.value
                        if "." not in name:
                            name = f"{namespace}.{name}"
                        deps.append(name)
        return deps
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

    # Map every provided address to its owning plan
    addr_to_plan: dict[str, "ConceptStatementPlan"] = {}
    for plan in concept_plans:
        for addr in plan.provided_addresses:
            addr_to_plan[addr] = plan

    # Use plan identity (id) as graph nodes
    plan_ids = {id(p): p for p in concept_plans}

    # Build dependency edges: plan -> set of plans it depends on
    dep_graph: dict[int, set[int]] = {id(p): set() for p in concept_plans}
    for plan in concept_plans:
        for dep_addr in plan.dependencies:
            dep_plan = addr_to_plan.get(dep_addr)
            if dep_plan is not None and id(dep_plan) != id(plan):
                dep_graph[id(plan)].add(id(dep_plan))

    # Kahn's algorithm
    forward: dict[int, list[int]] = defaultdict(list)
    in_deg: dict[int, int] = {}
    for pid, deps in dep_graph.items():
        in_deg[pid] = len(deps)
        for dep_pid in deps:
            forward[dep_pid].append(pid)

    queue: deque[int] = deque(pid for pid, deg in in_deg.items() if deg == 0)

    ordered: list["ConceptStatementPlan"] = []
    while queue:
        pid = queue.popleft()
        ordered.append(plan_ids[pid])
        for dependent in forward.get(pid, []):
            in_deg[dependent] -= 1
            if in_deg[dependent] == 0:
                queue.append(dependent)

    # Append any remaining (cycles) in original order
    seen = set(id(p) for p in ordered)
    for plan in concept_plans:
        if id(plan) not in seen:
            ordered.append(plan)

    return ordered


def _finalize_nested_selects(output: Any, environment: Environment) -> None:
    """Find and finalize any SelectStatements nested inside a hydrated output."""
    if isinstance(output, SelectStatement):
        output.finalize(environment)
    elif isinstance(output, PersistStatement):
        output.select.finalize(environment)
    elif isinstance(output, MultiSelectStatement):
        for sel in output.selects:
            sel.finalize(environment)


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
    provided_addresses: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        self.address = collect_concept_address(self.syntax, hydrator.environment)
        if self.address:
            self.provided_addresses = [self.address]
        else:
            # Properties blocks: extract all child concept names
            self.provided_addresses = collect_properties_addresses(
                self.syntax, hydrator.environment
            )

    def bind(self, hydrator: "NativeHydrator") -> None:
        self.dependencies = extract_dependencies(self.syntax, hydrator.environment)

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        # Concepts are created during BIND via _sort_and_create_concepts
        pass

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

    def bind(self, hydrator: "NativeHydrator") -> None:
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
    inline_addresses: list[str] = field(default_factory=list)

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        namespace = hydrator.environment.namespace or DEFAULT_NAMESPACE
        self.inline_addresses = collect_inline_concept_addresses(self.syntax, namespace)

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        if isinstance(self.output, SelectStatement):
            self.output.finalize(hydrator.environment)

    def commit(self, hydrator: "NativeHydrator") -> SelectStatement | None:
        return self.output


@dataclass
class FunctionDefinitionPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: FunctionDeclaration | None = None
    parameter_names: list[str] = field(default_factory=list)

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        self.parameter_names = FunctionDefinitionSyntax.from_node(
            self.syntax
        ).parameter_names

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        with temporary_function_scope(hydrator.environment, self.parameter_names):
            self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> FunctionDeclaration | None:
        return self.output


@dataclass
class DatasourceStatementPlan(StatementPlanBase):
    # Datasources are self-contained: build the output in hydrate, commit as-is.
    syntax: SyntaxNode
    output: Datasource | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> Datasource | None:
        return self.output


@dataclass
class MergeStatementPlan(StatementPlanBase):
    # Merge resolves concept addresses at hydrate time only.
    syntax: SyntaxNode
    output: MergeStatementV2 | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> MergeStatementV2 | None:
        return self.output


@dataclass
class RowsetStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: RowsetDerivationStatement | None = None
    rowset_name: str | None = None
    forward_addresses: list[str] = field(default_factory=list)

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        for child in self.syntax.children:
            if (
                isinstance(child, SyntaxToken)
                and child.kind == SyntaxTokenKind.IDENTIFIER
            ):
                self.rowset_name = child.value
                return

    def bind(self, hydrator: "NativeHydrator") -> None:
        # Forward references inside a rowset derive clause (e.g. coalesce(level0.qoh1, ...))
        # resolve to outputs that only exist after rowset_to_concepts runs. Stage deliberate
        # placeholders now; the real concepts replace them during hydrate.
        if not self.rowset_name:
            return
        namespace = hydrator.environment.namespace or DEFAULT_NAMESPACE
        prefixes = (
            f"{self.rowset_name}.",
            f"{namespace}.{self.rowset_name}.",
        )
        seen: set[str] = set()
        for literal in find_concept_literals(self.syntax):
            address = extract_concept_name_from_literal(literal, namespace)
            if not address.startswith(prefixes):
                continue
            if address in seen:
                continue
            seen.add(address)
            self.forward_addresses.append(address)

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        with temporary_rowset_scope(hydrator.environment, self.forward_addresses):
            self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> RowsetDerivationStatement | None:
        return self.output


@dataclass
class PersistStatementPlan(StatementPlanBase):
    # Persist wraps a SELECT; hydrate builds the statement, validate finalizes the nested select.
    syntax: SyntaxNode
    output: PersistStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        _finalize_nested_selects(self.output, hydrator.environment)

    def commit(self, hydrator: "NativeHydrator") -> PersistStatement | None:
        return self.output


@dataclass
class RawSQLStatementPlan(StatementPlanBase):
    # Raw SQL carries an opaque string; hydrate just extracts it.
    syntax: SyntaxNode
    output: RawSQLStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> RawSQLStatement | None:
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
        self.plans: list[StatementPlan] = []
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
        return [self.plan_form(form) for form in forms]

    _IMPORT_KINDS = {
        SyntaxNodeKind.IMPORT_STATEMENT,
        SyntaxNodeKind.SELECTIVE_IMPORT_STATEMENT,
        SyntaxNodeKind.SELF_IMPORT_STATEMENT,
    }

    _FUNCTION_INNER_KINDS = {
        SyntaxNodeKind.RAW_FUNCTION,
        SyntaxNodeKind.TABLE_FUNCTION,
    }

    _PERSIST_KINDS = {
        SyntaxNodeKind.PERSIST_STATEMENT,
        SyntaxNodeKind.AUTO_PERSIST,
        SyntaxNodeKind.FULL_PERSIST,
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
                return self._plan_function_block(statement)
            if statement.kind in self._IMPORT_KINDS:
                return ImportStatementPlan(statement)
            if statement.kind == SyntaxNodeKind.SELECT_STATEMENT:
                return SelectStatementPlan(statement)
            return self._plan_block_statement(statement)
        return UnsupportedStatementPlan(form)

    def _plan_function_block(self, statement: SyntaxNode) -> StatementPlan:
        inner = statement.children[0]
        if isinstance(inner, SyntaxNode) and inner.kind in self._FUNCTION_INNER_KINDS:
            return FunctionDefinitionPlan(inner)
        return UnsupportedStatementPlan(statement)

    def _plan_block_statement(self, statement: SyntaxNode) -> StatementPlan:
        kind = statement.kind
        if kind == SyntaxNodeKind.DATASOURCE:
            return DatasourceStatementPlan(statement)
        if kind == SyntaxNodeKind.MERGE_STATEMENT:
            return MergeStatementPlan(statement)
        if kind == SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT:
            return RowsetStatementPlan(statement)
        if kind in self._PERSIST_KINDS:
            return PersistStatementPlan(statement)
        if kind == SyntaxNodeKind.RAWSQL_STATEMENT:
            return RawSQLStatementPlan(statement)
        return UnsupportedStatementPlan(statement)

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
