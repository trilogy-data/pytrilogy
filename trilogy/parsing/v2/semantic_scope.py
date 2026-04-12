from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterator

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import Purpose
from trilogy.core.models.author import UndefinedConceptFull
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment


class ScopeKind(Enum):
    GLOBAL = "global"
    FUNCTION = "function"
    ROWSET = "rowset"
    IMPORT = "import"


@dataclass
class SymbolDefinition:
    address: str
    name: str
    namespace: str
    scope: "SemanticScope"
    materialized: bool = False


@dataclass
class SymbolReference:
    address: str
    scope: "SemanticScope"


@dataclass
class SemanticScope:
    kind: ScopeKind
    parent: "SemanticScope | None" = None
    definitions: dict[str, SymbolDefinition] = field(default_factory=dict)

    def lookup(self, address: str) -> SymbolDefinition | None:
        scope: SemanticScope | None = self
        while scope is not None:
            found = scope.definitions.get(address)
            if found is not None:
                return found
            scope = scope.parent
        return None


class SymbolTable:
    """v2 semantic symbol table.

    Owns a stack of SemanticScopes and is the single point that mirrors
    scoped declarations into ``environment.concepts.data`` as
    ``UndefinedConceptFull`` placeholders. Downstream code paths in
    ``trilogy.core`` (notably ``function_args_to_output_purpose``) look
    up ConceptRefs by address via ``environment.concepts[...]``, so a
    materialized placeholder is the current compatibility contract.
    This compatibility shim is intentionally isolated here; nothing
    else in ``trilogy.parsing.v2`` should mutate
    ``environment.concepts.data`` for scoped declarations.
    """

    def __init__(self, environment: Environment) -> None:
        self.environment = environment
        self.global_scope = SemanticScope(kind=ScopeKind.GLOBAL)
        self._stack: list[SemanticScope] = [self.global_scope]

    @property
    def current(self) -> SemanticScope:
        return self._stack[-1]

    def declare(
        self,
        address: str,
        name: str,
        namespace: str,
    ) -> SymbolDefinition:
        sym = SymbolDefinition(
            address=address,
            name=name,
            namespace=namespace,
            scope=self.current,
        )
        self.current.definitions[address] = sym
        return sym

    def lookup(self, address: str) -> SymbolDefinition | None:
        return self.current.lookup(address)

    def reference(self, address: str) -> SymbolReference | None:
        found = self.lookup(address)
        if found is None:
            return None
        return SymbolReference(address=address, scope=found.scope)

    @contextmanager
    def push_scope(self, kind: ScopeKind) -> Iterator[SemanticScope]:
        scope = SemanticScope(kind=kind, parent=self.current)
        self._stack.append(scope)
        try:
            yield scope
        finally:
            self._stack.pop()

    @contextmanager
    def function_scope(
        self, parameter_names: list[str]
    ) -> Iterator[SemanticScope]:
        namespace = self.environment.namespace or DEFAULT_NAMESPACE
        entries = [
            (f"{namespace}.{name}", name, namespace) for name in parameter_names
        ]
        with self.push_scope(ScopeKind.FUNCTION) as scope:
            materialized = self._materialize(scope, entries)
            try:
                yield scope
            finally:
                self._demote(materialized, only_if_undefined=False)

    @contextmanager
    def rowset_scope(self, addresses: list[str]) -> Iterator[SemanticScope]:
        namespace = self.environment.namespace or DEFAULT_NAMESPACE
        entries: list[tuple[str, str, str]] = []
        for address in addresses:
            if "." in address:
                ns, _, nm = address.rpartition(".")
            else:
                ns, nm = namespace, address
            entries.append((address, nm, ns))
        with self.push_scope(ScopeKind.ROWSET) as scope:
            # Rowset forward refs get replaced with real concepts via force=True
            # during hydrate; anything still undefined on exit is a placeholder
            # we planted and must remove.
            materialized = self._materialize(scope, entries)
            try:
                yield scope
            finally:
                self._demote(materialized, only_if_undefined=True)

    def _materialize(
        self,
        scope: SemanticScope,
        entries: list[tuple[str, str, str]],
    ) -> list[str]:
        added: list[str] = []
        for address, name, namespace in entries:
            sym = SymbolDefinition(
                address=address,
                name=name,
                namespace=namespace,
                scope=scope,
            )
            scope.definitions[address] = sym
            if address in self.environment.concepts.data:
                continue
            self.environment.concepts.data[address] = UndefinedConceptFull(
                name=name,
                namespace=namespace,
                datatype=DataType.UNKNOWN,
                purpose=Purpose.UNKNOWN,
            )
            sym.materialized = True
            added.append(address)
        return added

    def _demote(self, addresses: list[str], only_if_undefined: bool) -> None:
        for address in addresses:
            if only_if_undefined:
                concept = self.environment.concepts.data.get(address)
                if isinstance(concept, UndefinedConceptFull):
                    self.environment.concepts.data.pop(address, None)
            else:
                self.environment.concepts.data.pop(address, None)
