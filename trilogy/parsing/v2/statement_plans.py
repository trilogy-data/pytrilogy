from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.author import Comment, CustomFunctionFactory
from trilogy.core.models.datasource import Datasource
from trilogy.core.statements.author import (
    ChartStatement,
    ConceptDeclarationStatement,
    CopyStatement,
    CreateStatement,
    FunctionDeclaration,
    ImportStatement,
    MergeStatementV2,
    MockStatement,
    MultiSelectStatement,
    PersistStatement,
    PublishStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectStatement,
    ShowStatement,
    TypeDeclaration,
    ValidateStatement,
)
from trilogy.parsing.v2.function_syntax import FunctionDefinitionSyntax
from trilogy.parsing.v2.import_rules import (
    import_statement,
    selective_import_statement,
    self_import_statement,
)
from trilogy.parsing.v2.import_service import ImportRequest
from trilogy.parsing.v2.model import HydrationDiagnostic
from trilogy.parsing.v2.rowset_semantics import (
    apply_alias_updates,
    rowset_output_namespace,
)
from trilogy.parsing.v2.select_finalize import (
    finalize_select_tree as _v2_finalize_select_tree,
)
from trilogy.parsing.v2.symbols import (
    collect_concept_address,
    collect_inline_concept_addresses,
    collect_properties_addresses,
    extract_concept_name_from_literal,
    extract_dependencies,
    find_concept_literals,
    find_select_transform_targets,
)
from trilogy.parsing.v2.syntax import (
    SyntaxElement,
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
    syntax_name,
)

if TYPE_CHECKING:
    from trilogy.parsing.v2.hydration import NativeHydrator


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


class StatementPlan(Protocol):
    def load_imports(self, hydrator: "NativeHydrator") -> None: ...

    def collect_symbols(self, hydrator: "NativeHydrator") -> None: ...

    def bind(self, hydrator: "NativeHydrator") -> None: ...

    def hydrate(self, hydrator: "NativeHydrator") -> None: ...

    def validate(self, hydrator: "NativeHydrator") -> None: ...

    def commit(self, hydrator: "NativeHydrator") -> Any: ...


class StatementPlanBase:
    def load_imports(self, hydrator: "NativeHydrator") -> None:
        return None

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        return None

    def bind(self, hydrator: "NativeHydrator") -> None:
        return None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        return None

    def validate(self, hydrator: "NativeHydrator") -> None:
        return None


def finalize_select_tree(output: Any, hydrator: "NativeHydrator") -> None:
    """v2-native finalize entry point used by statement plans.

    Routes through ``select_finalize.finalize_select_tree`` so concept
    lookups resolve against ``RuleContext.concepts`` instead of reading
    directly from ``environment.concepts``.
    """
    _v2_finalize_select_tree(output, hydrator.rule_context())


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
            self.provided_addresses = collect_properties_addresses(
                self.syntax, hydrator.environment
            )
        for addr in self.provided_addresses:
            namespace, _, name = addr.rpartition(".")
            hydrator.symbol_table.declare(addr, name or addr, namespace or "")
        # Dependency addresses are extracted for the topological sort only.
        # They are NOT declared into the symbol table: a concept body may
        # only reference identifiers that have a real source (already in
        # env.concepts from a prior statement or import, or provided by
        # another top-level concept plan's collect_symbols). Declaring
        # arbitrary body literals here would authorize scoped placeholders
        # for wholly undeclared identifiers and silently accept broken
        # concept declarations — strict v2 parsing must raise instead.
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

    def validate(self, hydrator: "NativeHydrator") -> None:
        if self.output is None:
            return
        content = self.output.content
        if isinstance(content, (SelectStatement, PersistStatement)):
            finalize_select_tree(content, hydrator)

    def commit(self, hydrator: "NativeHydrator") -> ShowStatement | None:
        return self.output


@dataclass
class ImportStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: ImportStatement | None = None

    def load_imports(self, hydrator: "NativeHydrator") -> None:
        # Imports materialize in their own early phase because later
        # statements need the imported concepts/functions/datasources
        # resolvable during collect_symbols, bind, and hydrate. This is
        # not a generic commit - it is an intentional early binding of
        # another environment into this one.
        kind = self.syntax.kind
        context = hydrator.rule_context()
        if kind == SyntaxNodeKind.IMPORT_STATEMENT:
            request = import_statement(self.syntax, context, hydrator.hydrate_rule)
        elif kind == SyntaxNodeKind.SELECTIVE_IMPORT_STATEMENT:
            request = selective_import_statement(
                self.syntax, context, hydrator.hydrate_rule
            )
        elif kind == SyntaxNodeKind.SELF_IMPORT_STATEMENT:
            self.output = self_import_statement(
                self.syntax, context, hydrator.hydrate_rule
            )
            return
        else:
            return
        if isinstance(request, ImportRequest):
            self.output = hydrator.import_service.execute(request)

    def commit(self, hydrator: "NativeHydrator") -> ImportStatement | None:
        return self.output


def _declare_inline_literals(
    syntax: SyntaxNode,
    hydrator: "NativeHydrator",
    namespace: str,
) -> None:
    """Pre-declare every inline concept literal address as a scoped symbol.

    Mirrors v1's lazy ``fail_on_missing=False`` lookup behavior without any
    env mutation: select/rowset/multiselect bodies frequently reference
    concepts (or typo'd identifiers) before their declaration is hydrated,
    or that intentionally never resolve at all. Pre-declaring puts the
    address in the symbol table so ``ConceptLookup`` returns a
    non-durable scoped placeholder instead of raising. Real concepts
    later in the parse displace the placeholder via ``SemanticState.add``.
    """
    for literal in find_concept_literals(syntax):
        address = extract_concept_name_from_literal(literal, namespace)
        ns, _, nm = address.rpartition(".")
        hydrator.symbol_table.declare(address, nm or address, ns or namespace)


@dataclass
class SelectStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: SelectStatement | None = None
    inline_addresses: list[str] = field(default_factory=list)

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        namespace = hydrator.environment.namespace or DEFAULT_NAMESPACE
        self.inline_addresses = collect_inline_concept_addresses(self.syntax, namespace)
        for addr in self.inline_addresses:
            ns, _, nm = addr.rpartition(".")
            hydrator.symbol_table.declare(addr, nm or addr, ns or namespace)
        _declare_inline_literals(self.syntax, hydrator, namespace)

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        finalize_select_tree(self.output, hydrator)

    def commit(self, hydrator: "NativeHydrator") -> SelectStatement | None:
        return self.output


@dataclass
class MultiSelectStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: MultiSelectStatement | None = None
    inline_addresses: list[str] = field(default_factory=list)

    def collect_symbols(self, hydrator: "NativeHydrator") -> None:
        namespace = hydrator.environment.namespace or DEFAULT_NAMESPACE
        self.inline_addresses = collect_inline_concept_addresses(self.syntax, namespace)
        for addr in self.inline_addresses:
            ns, _, nm = addr.rpartition(".")
            hydrator.symbol_table.declare(addr, nm or addr, ns or namespace)
        _declare_inline_literals(self.syntax, hydrator, namespace)

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        finalize_select_tree(self.output, hydrator)

    def commit(self, hydrator: "NativeHydrator") -> MultiSelectStatement | None:
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

    def bind(self, hydrator: "NativeHydrator") -> None:
        # Register into env.functions during BIND so later concept plans
        # (hydrated inside _sort_and_create_concepts) can resolve @name refs.
        # The pending overlay exposes scoped placeholders staged by
        # ConceptLookup to v1 helpers (FunctionFactory / parsing.common)
        # that read environment.concepts directly during the same hydration.
        with hydrator.symbol_table.function_scope(
            self.parameter_names
        ), hydrator.semantic_state.pending_overlay_scope():
            self.output = hydrator.hydrate_rule(self.syntax)
        if self.output is None:
            return
        hydrator.environment.functions[self.output.name] = CustomFunctionFactory(
            function=self.output.expr,
            namespace=hydrator.environment.namespace,
            function_arguments=self.output.args,
            name=self.output.name,
        )

    def commit(self, hydrator: "NativeHydrator") -> FunctionDeclaration | None:
        return self.output


@dataclass
class DatasourceStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: Datasource | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> Datasource | None:
        if self.output is None:
            return None
        # Flush staged concepts before add_datasource so validate_concept
        # sees this parse's concept declarations as durable rather than
        # invalidating the just-bound datasource during the final commit.
        hydrator.semantic_state.commit(hydrator.environment)
        hydrator.environment.add_datasource(self.output)
        return self.output


@dataclass
class MergeStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: MergeStatementV2 | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> MergeStatementV2 | None:
        if self.output is None:
            return None
        for source_c in self.output.sources:
            hydrator.semantic_state.stage_merge(
                source_c,
                self.output.targets[source_c.address],
                self.output.modifiers,
            )
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
                break
        if not self.rowset_name:
            return
        # Declare scoped placeholders for the rowset's prospective output
        # addresses so concept declarations later in the same parse (which
        # hydrate during BIND, before this plan's hydrate runs) can resolve
        # ``rowset_name.<source_address>`` and ``rowset_name.<transform_name>``
        # as forward references. Real concepts created in
        # ``rowset_derivation_statement`` displace these via
        # ``SemanticState.add``.
        namespace = hydrator.environment.namespace or DEFAULT_NAMESPACE
        seen: set[str] = set()
        for literal in find_concept_literals(self.syntax):
            source_address = extract_concept_name_from_literal(literal, namespace)
            source_ns, _, source_name = source_address.rpartition(".")
            target_ns = rowset_output_namespace(self.rowset_name, namespace, source_ns)
            rowset_address = f"{target_ns}.{source_name}"
            if rowset_address in seen:
                continue
            seen.add(rowset_address)
            hydrator.symbol_table.declare(rowset_address, source_name, target_ns)
        for transform_name in find_select_transform_targets(self.syntax):
            target_ns = rowset_output_namespace(self.rowset_name, namespace, namespace)
            rowset_address = f"{target_ns}.{transform_name}"
            if rowset_address in seen:
                continue
            seen.add(rowset_address)
            hydrator.symbol_table.declare(rowset_address, transform_name, target_ns)

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
        with hydrator.symbol_table.rowset_scope(self.forward_addresses):
            self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> RowsetDerivationStatement | None:
        if self.output is None:
            return None
        alias_updates = hydrator.semantic_state.drain_rowset_aliases()
        apply_alias_updates(alias_updates, hydrator.environment)
        hydrator.environment.add_rowset(
            self.output.name,
            self.output.select.as_lineage(hydrator.environment),
        )
        return self.output


@dataclass
class PersistStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: PersistStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        finalize_select_tree(self.output, hydrator)

    def commit(self, hydrator: "NativeHydrator") -> PersistStatement | None:
        return self.output


@dataclass
class RawSQLStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: RawSQLStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> RawSQLStatement | None:
        return self.output


@dataclass
class TypeDeclarationPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: TypeDeclaration | None = None

    def bind(self, hydrator: "NativeHydrator") -> None:
        # Stage before concept hydration so `key revenue float::money;`
        # can resolve the trait through the type lookup facade when
        # _sort_and_create_concepts runs after BIND. Durable writes into
        # environment.data_types happen in semantic_state.commit.
        self.output = hydrator.hydrate_rule(self.syntax)
        if self.output is not None:
            hydrator.semantic_state.add_type(self.output.type)

    def commit(self, hydrator: "NativeHydrator") -> TypeDeclaration | None:
        return self.output


@dataclass
class SimpleOperationalStatementPlan(StatementPlanBase):
    """Shared plan for simple operational top-level statements.

    Used for create/validate/mock/publish statements that do not
    introduce concepts, datasources, or select trees.
    """

    syntax: SyntaxNode
    output: Any = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> Any:
        return self.output


@dataclass
class CreateStatementPlan(SimpleOperationalStatementPlan):
    output: CreateStatement | None = None


@dataclass
class ValidateStatementPlan(SimpleOperationalStatementPlan):
    output: ValidateStatement | None = None


@dataclass
class MockStatementPlan(SimpleOperationalStatementPlan):
    output: MockStatement | None = None


@dataclass
class PublishStatementPlan(SimpleOperationalStatementPlan):
    output: PublishStatement | None = None


@dataclass
class CopyStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: CopyStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        if self.output is not None:
            finalize_select_tree(self.output.select, hydrator)

    def commit(self, hydrator: "NativeHydrator") -> CopyStatement | None:
        return self.output


@dataclass
class ChartStatementPlan(StatementPlanBase):
    syntax: SyntaxNode
    output: ChartStatement | None = None

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        if self.output is not None:
            finalize_select_tree(self.output.select, hydrator)

    def commit(self, hydrator: "NativeHydrator") -> ChartStatement | None:
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
