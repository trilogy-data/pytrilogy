from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Iterator

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.author import Concept, ConceptRef
from trilogy.core.models.environment import Environment


class ConceptUpdateKind(Enum):
    TOP_LEVEL_DECLARATION = "top_level_declaration"
    PROPERTY_DECLARATION = "property_declaration"
    DATASOURCE_PROPERTY = "datasource_property"
    SELECT_LOCAL = "select_local"
    MULTISELECT_OUTPUT = "multiselect_output"
    ROWSET_OUTPUT = "rowset_output"
    VIRTUAL_HELPER = "virtual_helper"


@dataclass
class ConceptUpdate:
    concept: Concept
    kind: ConceptUpdateKind
    meta: Any | None = None


class _Absent:
    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<absent>"


_ABSENT: Any = _Absent()


@dataclass
class _MirrorEntry:
    address: str
    prior: Any


@dataclass
class SemanticState:
    """Parser-owned concept state with explicit commit/rollback.

    v2 hydration must make newly declared concepts visible to later rules in
    the same parse. SemanticState records each pending write with its intent
    (``ConceptUpdateKind``) and mirrors it into ``environment.concepts`` so
    existing address-based lookups still resolve. The mirror is bookkept so
    ``rollback`` can reverse compatibility writes if the parse fails partway
    through.

    ``concepts`` accumulates the full update history across committed batches
    so callers can inspect what was applied. The in-flight batch is bounded
    by ``_pending_start``; ``commit`` advances that boundary and ``rollback``
    drops the in-flight entries and restores the mirrored environment keys.
    """

    environment: Environment
    concepts: list[ConceptUpdate] = field(default_factory=list)
    mirror_to_environment: bool = True
    _mirror: list[_MirrorEntry] = field(default_factory=list)
    _seen: set[str] = field(default_factory=set)
    _pending_by_address: dict[str, Concept] = field(default_factory=dict)
    _pending_start: int = 0
    _visible_depth: int = 0
    _visible_writes: list[_MirrorEntry] = field(default_factory=list)
    _visible_seen: set[str] = field(default_factory=set)
    _pending_rowset_aliases: list[Any] = field(default_factory=list)

    def add(
        self,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
        force: bool = False,
    ) -> Concept:
        address = concept.address
        if self.mirror_to_environment:
            if address not in self._seen:
                # Read through the raw dict to avoid EnvironmentConceptDict's
                # namespace/fallback resolution muddying the rollback snapshot.
                prior = self.environment.concepts.data.get(address, _ABSENT)
                self._mirror.append(_MirrorEntry(address=address, prior=prior))
                self._seen.add(address)
            resolved = self.environment.add_concept(concept, meta, force=force)
        else:
            existing = self._pending_by_address.get(address)
            if existing is not None and not force:
                resolved = existing
            else:
                resolved = concept
            if self._visible_depth > 0:
                self._expose_to_environment(address, resolved)
        self._pending_by_address[address] = resolved
        self.concepts.append(ConceptUpdate(concept=resolved, kind=kind, meta=meta))
        return resolved

    def _expose_to_environment(self, address: str, concept: Concept) -> None:
        data = self.environment.concepts.data
        if address not in self._visible_seen:
            prior = data.get(address, _ABSENT)
            self._visible_writes.append(_MirrorEntry(address=address, prior=prior))
            self._visible_seen.add(address)
        data[address] = concept

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
        if self.mirror_to_environment:
            if address not in self._seen:
                prior = self.environment.concepts.data.get(address, _ABSENT)
                self._mirror.append(_MirrorEntry(address=address, prior=prior))
                self._seen.add(address)
            resolved = self.environment.add_concept(concept, meta, force=True)
        else:
            resolved = concept
            if self._visible_depth > 0:
                self._expose_to_environment(address, resolved)
        self._pending_by_address[address] = resolved
        self.concepts.append(ConceptUpdate(concept=resolved, kind=kind, meta=meta))
        return resolved

    @contextmanager
    def visible_in_environment(self) -> Iterator[None]:
        """Temporarily expose pending concepts via ``environment.concepts.data``.

        Compatibility scaffold: v1 helper functions in ``trilogy.parsing.common``
        resolve ConceptRefs via ``environment.concepts[...]``. When the mirror
        is disabled, pending concepts are not yet visible there. v2 rule code
        wraps a phase (hydrate/validate) with this context manager so v1
        helpers called within can resolve pending concepts. Concepts added via
        ``add`` while the scope is active are also exposed on the fly. All
        env writes made by this scaffold are reverted on exit of the outer
        scope so parse-time env mutation does not leak. This lives inside
        SemanticState so the documented compatibility exception applies.
        """
        if self.mirror_to_environment:
            yield
            return
        outermost = self._visible_depth == 0
        self._visible_depth += 1
        if outermost:
            for address, concept in self._pending_by_address.items():
                self._expose_to_environment(address, concept)
        try:
            yield
        finally:
            self._visible_depth -= 1
            if self._visible_depth == 0:
                data = self.environment.concepts.data
                for entry in reversed(self._visible_writes):
                    if entry.prior is _ABSENT:
                        data.pop(entry.address, None)
                    else:
                        data[entry.address] = entry.prior
                self._visible_writes.clear()
                self._visible_seen.clear()

    def lookup(self, address: str) -> Concept | None:
        pending = self._pending_by_address.get(address)
        if pending is not None:
            return pending
        return self.environment.concepts.data.get(address)

    def pending_lookup(self, address: str) -> Concept | None:
        return self._pending_by_address.get(address)

    def pending_concepts(self) -> Iterable[tuple[str, Concept]]:
        return self._pending_by_address.items()

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
        committed = self.concepts[self._pending_start :]
        if not self.mirror_to_environment:
            # When mirroring is disabled, the environment has not yet seen
            # the pending concepts. Apply them now so downstream consumers
            # (executor, rendering, etc.) can resolve them by address.
            for update in committed:
                self.environment.add_concept(
                    update.concept, meta=update.meta, force=True
                )
        self._mirror.clear()
        self._seen.clear()
        self._pending_by_address.clear()
        self._pending_start = len(self.concepts)
        return list(committed)

    def rollback(self) -> None:
        if self.mirror_to_environment:
            data = self.environment.concepts.data
            for entry in reversed(self._mirror):
                if entry.prior is _ABSENT:
                    data.pop(entry.address, None)
                else:
                    data[entry.address] = entry.prior
        del self.concepts[self._pending_start :]
        self._mirror.clear()
        self._seen.clear()
        self._pending_by_address.clear()
        self._pending_rowset_aliases.clear()


class ConceptLookup:
    """Parser-owned concept lookup facade.

    Resolves concepts by address, preferring the current parse's in-flight
    (uncommitted) concepts from ``SemanticState`` and falling back to the
    base ``Environment``. Rule modules in ``trilogy.parsing.v2`` should go
    through this instead of reading ``environment.concepts`` directly, so
    we can eventually stop mirroring pending writes into the environment.
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
        elif "." not in address:
            candidates.append(prefix + address)
        return candidates

    def require(self, address: str) -> Concept:
        for candidate in self._candidate_addresses(address):
            pending = self._state.pending_lookup(candidate)
            if pending is not None:
                return pending
        return self._env.concepts[address]  # type: ignore[return-value]

    def get(self, address: str) -> Concept | None:
        for candidate in self._candidate_addresses(address):
            pending = self._state.pending_lookup(candidate)
            if pending is not None:
                return pending
        return self._env.concepts.get(address)

    def contains(self, address: str) -> bool:
        for candidate in self._candidate_addresses(address):
            if self._state.pending_lookup(candidate) is not None:
                return True
        return address in self._env.concepts

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
