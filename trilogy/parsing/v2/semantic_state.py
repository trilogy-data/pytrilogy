from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Iterator

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.author import Concept, ConceptRef, CustomType
from trilogy.core.models.environment import Environment


class ConceptUpdateKind(Enum):
    TOP_LEVEL_DECLARATION = "top_level_declaration"
    PROPERTY_DECLARATION = "property_declaration"
    DATASOURCE_PROPERTY = "datasource_property"
    AUTO_DERIVED = "auto_derived"
    SELECT_LOCAL = "select_local"
    MULTISELECT_OUTPUT = "multiselect_output"
    ROWSET_OUTPUT = "rowset_output"
    VIRTUAL_HELPER = "virtual_helper"


@dataclass
class ConceptUpdate:
    concept: Concept
    kind: ConceptUpdateKind
    meta: Any | None = None


@dataclass
class SemanticState:
    """Parser-owned concept state with staged commit/rollback.

    v2 hydration stages every newly declared concept in ``_pending_by_address``
    so later rules can resolve them via :class:`ConceptLookup` without mutating
    the base ``Environment``. ``commit`` is the only durable write path: it
    applies pending updates through ``Environment.add_concept``. ``rollback``
    drops pending state without touching the environment.

    ``concepts`` accumulates the full update history across committed batches
    so callers can inspect what was applied. The in-flight batch is bounded by
    ``_pending_start``; ``commit`` advances that boundary.

    ``pending_overlay_scope`` installs a read-only overlay on the
    environment's concept dict so v1 helper code in
    ``trilogy.parsing.common`` — which still resolves pending concept refs
    against the environment — sees pending concepts without any mutation
    of the underlying concept store. The overlay is backed by
    ``MappingProxyType`` and cannot be used as a write path. It is always
    popped on scope exit, even on exception.
    """

    environment: Environment
    concepts: list[ConceptUpdate] = field(default_factory=list)
    _pending_by_address: dict[str, Concept] = field(default_factory=dict)
    _pending_start: int = 0
    _pending_rowset_aliases: list[Any] = field(default_factory=list)
    _pending_types_by_name: dict[str, CustomType] = field(default_factory=dict)

    def add(
        self,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
        force: bool = False,
    ) -> Concept:
        address = concept.address
        existing = self._pending_by_address.get(address)
        if existing is not None and not force:
            resolved = existing
        else:
            resolved = concept
        self._pending_by_address[address] = resolved
        self.concepts.append(ConceptUpdate(concept=resolved, kind=kind, meta=meta))
        return resolved

    def replace_concept(
        self,
        address: str,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
    ) -> Concept:
        """Replace a previously-added pending concept with a new version.

        Used when a helper (e.g. select finalization) needs to update a
        concept after its initial registration — for example after the
        grain is known. Emits a new ConceptUpdate entry so downstream
        bookkeeping continues to see the canonical address.
        """
        self._pending_by_address[address] = concept
        self.concepts.append(ConceptUpdate(concept=concept, kind=kind, meta=meta))
        return concept

    @contextmanager
    def pending_overlay_scope(self) -> Iterator[None]:
        """Install pending concepts as a read-only overlay on the env concept dict.

        v1 helper functions in ``trilogy.parsing.common`` resolve concept
        references via the environment's concept dict. v2 rule code wraps a
        phase (hydrate/validate) with this context manager so those helpers
        see pending concepts without any parse-time mutation of the
        underlying concept store. The overlay is a live ``MappingProxyType``
        view of ``_pending_by_address`` — new concepts added via ``add``
        during the scope become visible on the fly, and the overlay is
        always removed on exit via ``EnvironmentConceptDict.push_overlay``'s
        ``finally`` clause.
        """
        with self.environment.concepts.push_overlay(self._pending_by_address):
            yield

    def pending_lookup(self, address: str) -> Concept | None:
        return self._pending_by_address.get(address)

    def pending_concepts(self) -> Iterable[tuple[str, Concept]]:
        return self._pending_by_address.items()

    def add_type(self, type_: CustomType) -> CustomType:
        self._pending_types_by_name[type_.name] = type_
        return type_

    def pending_type(self, name: str) -> CustomType | None:
        return self._pending_types_by_name.get(name)

    def pending_types(self) -> Iterable[tuple[str, CustomType]]:
        return self._pending_types_by_name.items()

    def stage_rowset_aliases(self, updates: list[Any]) -> None:
        self._pending_rowset_aliases.extend(updates)

    def drain_rowset_aliases(self) -> list[Any]:
        drained = list(self._pending_rowset_aliases)
        self._pending_rowset_aliases.clear()
        return drained

    def pending(self) -> Iterator[ConceptUpdate]:
        return iter(self.concepts[self._pending_start :])

    def commit(self, environment: Environment | None = None) -> list[ConceptUpdate]:
        if environment is not None and environment is not self.environment:
            raise ValueError("SemanticState committed against a different environment")
        # Types commit before concepts so concept hydration downstream sees
        # durable data_types entries through the same fallback path as imports.
        for name, type_ in self._pending_types_by_name.items():
            self.environment.data_types[name] = type_
        self._pending_types_by_name.clear()
        committed = self.concepts[self._pending_start :]
        for update in committed:
            self.environment.add_concept(update.concept, meta=update.meta, force=True)
        self._pending_by_address.clear()
        self._pending_start = len(self.concepts)
        return list(committed)

    def rollback(self) -> None:
        del self.concepts[self._pending_start :]
        self._pending_by_address.clear()
        self._pending_rowset_aliases.clear()
        self._pending_types_by_name.clear()


class ConceptLookup:
    """Parser-owned concept lookup facade.

    Resolves concepts by address, preferring the current parse's in-flight
    (uncommitted) concepts from ``SemanticState`` and falling back to the
    base ``Environment``. Rule modules in ``trilogy.parsing.v2`` should go
    through this instead of reading ``environment.concepts`` directly, so
    pending writes never have to leak into the environment.
    """

    __slots__ = ("_state", "_env")

    def __init__(self, state: SemanticState) -> None:
        self._state = state
        self._env = state.environment

    def _candidate_addresses(self, address: str) -> list[str]:
        # Mirror EnvironmentConceptDict.__getitem__ fallback: exact, strip
        # leading ``local.``, or qualify unnamespaced keys with ``local.``.
        candidates = [address]
        prefix = f"{DEFAULT_NAMESPACE}."
        if address.startswith(prefix):
            candidates.append(address[len(prefix) :])
        else:
            candidates.append(prefix + address)
        return candidates

    def _existing_concept(self, address: str) -> Concept | None:
        for candidate in self._candidate_addresses(address):
            pending = self._state.pending_lookup(candidate)
            if pending is not None:
                return pending
            committed = self._env.concepts.data.get(candidate)
            if committed is not None:
                return committed
        return None

    def _auto_derive(self, address: str) -> Concept | None:
        if "." not in address:
            return None
        parent_address, suffix = address.rsplit(".", 1)
        parent = self._existing_concept(parent_address)
        if parent is None:
            return None

        from trilogy.core.functions import try_create_auto_derived

        derived = try_create_auto_derived(parent, suffix, environment=self._env)
        if derived is None:
            return None
        return self._state.add(derived, ConceptUpdateKind.AUTO_DERIVED)

    def require(self, address: str) -> Concept:
        existing = self._existing_concept(address)
        if existing is not None:
            return existing
        for candidate in self._candidate_addresses(address):
            derived = self._auto_derive(candidate)
            if derived is not None:
                return derived
        return self._env.concepts[address]  # type: ignore[return-value]

    def get(self, address: str) -> Concept | None:
        existing = self._existing_concept(address)
        if existing is not None:
            return existing
        for candidate in self._candidate_addresses(address):
            derived = self._auto_derive(candidate)
            if derived is not None:
                return derived
        return None

    def contains(self, address: str) -> bool:
        return self._existing_concept(address) is not None

    def __contains__(self, address: object) -> bool:
        return isinstance(address, str) and self.contains(address)

    def __getitem__(self, address: str) -> Concept:
        return self.require(address)

    def values(self) -> list[Concept]:
        merged: dict[str, Concept] = {}
        for concept in self._env.concepts.values():
            merged[concept.address] = concept
        for address, concept in self._state.pending_concepts():
            merged[address] = concept
        return list(merged.values())

    def reference(self, address: str) -> ConceptRef:
        return self.require(address).reference


class TypeLookup:
    """Parser-owned custom-type lookup facade.

    Resolves ``CustomType`` entries by name, preferring the current parse's
    staged types from ``SemanticState`` and falling back to the environment's
    ``data_types`` dict. Rule modules in ``trilogy.parsing.v2`` should route
    type reads through this so pending type declarations are never written
    into the environment before a parse successfully commits.
    """

    __slots__ = ("_state", "_env")

    def __init__(self, state: SemanticState) -> None:
        self._state = state
        self._env = state.environment

    def get(self, name: str) -> CustomType | None:
        pending = self._state.pending_type(name)
        if pending is not None:
            return pending
        return self._env.data_types.get(name)

    def require(self, name: str) -> CustomType:
        found = self.get(name)
        if found is None:
            raise KeyError(name)
        return found

    def contains(self, name: str) -> bool:
        if self._state.pending_type(name) is not None:
            return True
        return name in self._env.data_types

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and self.contains(name)

    def __getitem__(self, name: str) -> CustomType:
        return self.require(name)
