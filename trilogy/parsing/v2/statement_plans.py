from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.author import Comment, CustomFunctionFactory
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
from trilogy.parsing.v2.function_syntax import FunctionDefinitionSyntax
from trilogy.parsing.v2.import_rules import (
    import_statement,
    selective_import_statement,
    self_import_statement,
)
from trilogy.parsing.v2.import_service import ImportRequest
from trilogy.parsing.v2.model import HydrationDiagnostic
from trilogy.parsing.v2.symbols import (
    collect_concept_address,
    collect_inline_concept_addresses,
    collect_properties_addresses,
    extract_concept_name_from_literal,
    extract_dependencies,
    find_concept_literals,
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


def finalize_select_tree(output: Any, environment: Environment) -> None:
    """Finalize any SelectStatements nested inside a hydrated output.

    Mirrors v1's eager SelectStatement.from_inputs behavior for nested
    contexts (multi-select, persist, rowset) so that `as_lineage` and
    downstream lineage conversion see populated grain/local_concepts.
    """
    if output is None:
        return
    if isinstance(output, SelectStatement):
        output.finalize(environment)
    elif isinstance(output, MultiSelectStatement):
        for sel in output.selects:
            sel.finalize(environment)
    elif isinstance(output, PersistStatement):
        finalize_select_tree(output.select, environment)
    elif isinstance(output, RowsetDerivationStatement):
        finalize_select_tree(output.select, environment)


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

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        finalize_select_tree(self.output, hydrator.environment)

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

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        self.output = hydrator.hydrate_rule(self.syntax)

    def validate(self, hydrator: "NativeHydrator") -> None:
        finalize_select_tree(self.output, hydrator.environment)

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

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        with hydrator.symbol_table.function_scope(self.parameter_names):
            self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> FunctionDeclaration | None:
        if self.output is None:
            return None
        hydrator.environment.functions[self.output.name] = CustomFunctionFactory(
            function=self.output.expr,
            namespace=hydrator.environment.namespace,
            function_arguments=self.output.args,
            name=self.output.name,
        )
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
            hydrator.environment.merge_concept(
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
        with hydrator.symbol_table.rowset_scope(self.forward_addresses):
            self.output = hydrator.hydrate_rule(self.syntax)

    def commit(self, hydrator: "NativeHydrator") -> RowsetDerivationStatement | None:
        if self.output is None:
            return None
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
        finalize_select_tree(self.output, hydrator.environment)

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
class UnsupportedStatementPlan(StatementPlanBase):
    syntax: SyntaxElement

    def hydrate(self, hydrator: "NativeHydrator") -> None:
        raise UnsupportedSyntaxError.from_syntax(
            f"No v2 statement plan for syntax node '{syntax_name(self.syntax)}'",
            self.syntax,
        )

    def commit(self, hydrator: "NativeHydrator") -> Any:
        return None
