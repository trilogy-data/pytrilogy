from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Never

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import Modifier, Purpose
from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.author import (
    Concept,
    ConceptRef,
    CustomType,
    UndefinedConceptFull,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment, _is_subsequence

if TYPE_CHECKING:
    from trilogy.parsing.v2.semantic_scope import SymbolTable


class ConceptUpdateKind(Enum):
    TOP_LEVEL_DECLARATION = "top_level_declaration"
    PROPERTY_DECLARATION = "property_declaration"
    DATASOURCE_PROPERTY = "datasource_property"
    AUTO_DERIVED = "auto_derived"
    SELECT_LOCAL = "select_local"
    MULTISELECT_OUTPUT = "multiselect_output"
    ROWSET_OUTPUT = "rowset_output"
    VIRTUAL_HELPER = "virtual_helper"
    # Staged pending-only placeholder for a struct field concept. These are
    # dropped on commit because ``environment.add_concept`` regenerates them
    # (with proper pseudonyms) via ``generate_related_concepts`` when the
    # parent struct concept is committed.
    STRUCT_FIELD_VIRTUAL = "struct_field_virtual"


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
    # Explicit `merge X into Y;` statements deferred until after concept
    # commit when either side is pending. Merge statements are stored as
    # environment-level build-time joins, so they share the scoped join machinery
    # instead of rewriting the author environment during parse.
    _pending_merges: list[tuple[Concept, Concept, list[Modifier]]] = field(
        default_factory=list
    )
    # Aliases whose import parse was short-circuited because a cycle was
    # detected in ImportHydrationService. Concept references into one of
    # these namespaces resolve to narrow UndefinedConceptFull placeholders
    # via ConceptLookup so the local parse can complete. These placeholders
    # only survive in the cycle-broken sub-environment; a successful
    # import higher up in the call stack materializes the real concepts
    # through the normal ``environment.add_import`` path.
    _deferred_import_aliases: set[str] = field(default_factory=set)
    # `self import as X` statements deferred until after the current parse's
    # concepts and datasources are durable. v2 load_imports runs before
    # concept hydration so the current env has nothing to copy yet; we stage
    # the alias here and materialize it via environment.add_import after the
    # final semantic_state.commit. Datasource columns that reference the
    # alias (e.g. parent.id) resolve to deferred placeholders during hydrate
    # via _deferred_import_aliases, then get overwritten when the real
    # concepts are copied in under the alias.
    _pending_self_imports: list[tuple[str, Path | None]] = field(default_factory=list)
    # Addresses staged via ``stage_placeholder``: scoped forward references
    # produced during function/select/rowset hydration whose target concept
    # is declared in the current parse but not yet hydrated. These live in
    # ``_pending_by_address`` (so the env overlay surfaces them to v1
    # helpers) but are NOT recorded in ``concepts`` — commit only walks
    # ``concepts``, so placeholders never leak into the durable concept
    # store. ``add`` clears the entry when a real concept later resolves
    # to the same address.
    _placeholder_addresses: set[str] = field(default_factory=set)
    # Names of rowsets whose SELECT body is currently being hydrated. A
    # SELECT alias (`expr -> name`) declared inside `with X as ...` must be
    # scoped to that rowset, not shared in the global `local` namespace —
    # otherwise two rowsets aliasing different expressions to the same name
    # clobber each other. While this stack is non-empty, the alias is
    # created (and resolved) under a hidden per-rowset name. A stack (not a
    # single value) because a rowset's MERGE body spans multiple SELECTs.
    _rowset_name_stack: list[str] = field(default_factory=list)

    @contextmanager
    def rowset_alias_scope(self, rowset_name: str) -> Iterator[None]:
        """Mark the enclosed hydration as being inside rowset ``rowset_name``.

        SELECT aliases hydrated within this scope are namespaced to the
        rowset (see :meth:`mangle_rowset_alias`) so they cannot collide
        with another rowset's identically-named alias.
        """
        self._rowset_name_stack.append(rowset_name)
        try:
            yield
        finally:
            self._rowset_name_stack.pop()

    @property
    def current_rowset_name(self) -> str | None:
        return self._rowset_name_stack[-1] if self._rowset_name_stack else None

    @staticmethod
    def mangle_rowset_alias(rowset_name: str, name: str) -> str:
        """Hidden, per-rowset bare name for a rowset SELECT alias.

        Single source of truth for the scheme; ``_rowset_concept`` strips
        the exact ``_{rowset_name}_`` prefix to recover the user-facing
        rowset-output name.
        """
        return f"_{rowset_name}_{name}"

    def add(
        self,
        concept: Concept,
        kind: ConceptUpdateKind,
        meta: Any | None = None,
        force: bool = False,
    ) -> Concept:
        address = concept.address
        existing = self._pending_by_address.get(address)
        is_placeholder = address in self._placeholder_addresses
        if existing is not None and not force and not is_placeholder:
            resolved = existing
        else:
            resolved = concept
        self._pending_by_address[address] = resolved
        self._placeholder_addresses.discard(address)
        self.concepts.append(ConceptUpdate(concept=resolved, kind=kind, meta=meta))
        return resolved

    def stage_placeholder(self, concept: Concept) -> Concept:
        """Stage a non-durable scoped placeholder concept.

        Used for forward references inside function/select/rowset bodies whose
        target concept is declared in the current parse but has not yet been
        hydrated. The placeholder lives only in ``_pending_by_address`` so it
        is visible to ``ConceptLookup`` and the env overlay during the parse.
        It is NOT recorded in ``concepts``, so ``commit`` (which iterates
        ``concepts``) never persists it. If a real concept later hydrates to
        the same address, ``add`` displaces the placeholder atomically.
        """
        address = concept.address
        existing = self._pending_by_address.get(address)
        if existing is not None and address not in self._placeholder_addresses:
            return existing
        self._pending_by_address[address] = concept
        self._placeholder_addresses.add(address)
        return concept

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

    def stage_field_alias(self, address: str, target: Concept) -> None:
        """Point a pending address at an existing canonical concept.

        Used for struct field concepts where the field value is already a
        declared Concept (e.g. ``struct<a,b>`` referencing ``local.a``):
        the field address ``wrapper.a`` should resolve to the ``local.a``
        concept itself, mirroring v1's post-``merge_concept`` state. No
        ``ConceptUpdate`` is emitted — final commit regenerates these via
        ``environment.add_concept`` on the parent struct.
        """
        self._pending_by_address[address] = target

    def pending_concepts(self) -> Iterable[tuple[str, Concept]]:
        return self._pending_by_address.items()

    def add_deferred_import_alias(self, alias: str) -> None:
        self._deferred_import_aliases.add(alias)

    def add_pending_self_import(self, alias: str, path: Path | None) -> None:
        self._pending_self_imports.append((alias, path))

    def drain_pending_self_imports(self) -> list[tuple[str, Path | None]]:
        drained = list(self._pending_self_imports)
        self._pending_self_imports.clear()
        return drained

    @property
    def deferred_import_aliases(self) -> set[str]:
        return self._deferred_import_aliases

    def add_type(self, type_: CustomType) -> CustomType:
        self._pending_types_by_name[type_.name] = type_
        return type_

    def pending_type(self, name: str) -> CustomType | None:
        return self._pending_types_by_name.get(name)

    def pending_types(self) -> Iterable[tuple[str, CustomType]]:
        return self._pending_types_by_name.items()

    def stage_merge(
        self,
        source: Concept,
        target: Concept,
        modifiers: list[Modifier],
    ) -> None:
        # Register eagerly when both sides are already durable. When either
        # side is pending, defer until commit so rollback does not leak a merge
        # join for concepts that were never durably added.
        durable = self.environment.concepts.data
        if (
            source.address in durable
            and target.address in durable
            and source.address not in self._pending_by_address
            and target.address not in self._pending_by_address
        ):
            self.environment.add_merge_join(source, target, modifiers)
            return
        self._pending_merges.append((source, target, modifiers))

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
        # Durable writes must see the canonical env.concepts, so detach any
        # live pending overlays before committing concepts and merge joins.
        with self.environment.concepts.without_overlays():
            # Types commit before concepts so concept hydration downstream
            # sees durable data_types entries through the same fallback
            # path as imports.
            for name, type_ in self._pending_types_by_name.items():
                self.environment.data_types[name] = type_
            self._pending_types_by_name.clear()
            committed = self.concepts[self._pending_start :]
            for update in committed:
                if update.kind == ConceptUpdateKind.STRUCT_FIELD_VIRTUAL:
                    # Regenerated by generate_related_concepts on the parent's
                    # commit — skip so the pseudonym-enriched copy wins.
                    continue
                self.environment.add_concept(
                    update.concept,
                    meta=update.meta,
                    force=update.kind != ConceptUpdateKind.TOP_LEVEL_DECLARATION,
                )
            # Explicit merge statements apply only after concepts are durable
            # when their concepts were pending during plan commit.
            pending_merges = self._pending_merges
            self._pending_merges = []
            for source, target, modifiers in pending_merges:
                self.environment.add_merge_join(source, target, modifiers)
            self._pending_by_address.clear()
            self._placeholder_addresses.clear()
            self._pending_start = len(self.concepts)
            return list(committed)

    def rollback(self) -> None:
        del self.concepts[self._pending_start :]
        self._pending_by_address.clear()
        self._placeholder_addresses.clear()
        self._pending_rowset_aliases.clear()
        self._pending_types_by_name.clear()
        self._deferred_import_aliases.clear()
        self._pending_merges.clear()
        self._pending_self_imports.clear()


class ConceptLookup:
    """Parser-owned concept lookup facade.

    Resolves concepts by address, preferring the current parse's in-flight
    (uncommitted) concepts from ``SemanticState`` and falling back to the
    base ``Environment``. Rule modules in ``trilogy.parsing.v2`` should go
    through this instead of reading ``environment.concepts`` directly, so
    pending writes never have to leak into the environment.

    When wired with a ``SymbolTable``, also resolves *scoped placeholders*:
    forward references whose target is declared in the current parse but
    not yet hydrated, and dotted children of materialized scoped concepts
    (function parameters, rowset forward refs). Placeholders are staged
    via :meth:`SemanticState.stage_placeholder` and never commit.
    """

    __slots__ = ("_state", "_env", "_symbol_table")

    def __init__(
        self,
        state: SemanticState,
        symbol_table: "SymbolTable | None" = None,
    ) -> None:
        self._state = state
        self._env = state.environment
        self._symbol_table = symbol_table

    def _candidate_addresses(self, address: str) -> list[str]:
        # Mirror EnvironmentConceptDict.__getitem__ fallback: exact, strip
        # leading ``local.``, or qualify unnamespaced keys with ``local.``.
        prefix = f"{DEFAULT_NAMESPACE}."
        candidates: list[str] = []
        # Inside a rowset body, a default-namespace name (bare, or
        # ``local.x``) must prefer that rowset's hidden alias if one
        # exists — so HAVING/WHERE/ORDER BY/window references bind to the
        # same per-rowset concept the SELECT created, not a same-named
        # concept in the shared default namespace. The mangled name is
        # unique per rowset, so trying it first is safe: it only resolves
        # when this rowset actually declared that alias; otherwise
        # ``_existing_concept`` skips it and normal resolution proceeds
        # (so dotted refs and genuine top-level concepts are unaffected).
        rowset_name = self._state.current_rowset_name
        if rowset_name is not None:
            bare = address[len(prefix) :] if address.startswith(prefix) else address
            if "." not in bare:
                mangled = self._state.mangle_rowset_alias(rowset_name, bare)
                candidates.append(f"{prefix}{mangled}")
                candidates.append(mangled)
        candidates.append(address)
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

    def _make_scoped_placeholder(self, address: str) -> Concept:
        cached = self._state.pending_lookup(address)
        if isinstance(cached, UndefinedConceptFull):
            return cached
        namespace, _, name = address.rpartition(".")
        placeholder = UndefinedConceptFull(
            name=name or address,
            namespace=namespace or DEFAULT_NAMESPACE,
            datatype=DataType.UNKNOWN,
            purpose=Purpose.UNKNOWN,
        )
        self._state.stage_placeholder(placeholder)
        return placeholder

    def _scoped_placeholder(self, address: str) -> Concept | None:
        """Return a non-durable placeholder for forward / scoped references.

        Two cases:

        1. The address (or a candidate) is declared in the symbol table by
           an earlier ``collect_symbols`` pass — e.g. a top-level concept
           referenced before its hydration plan runs, or a select inline
           identifier referenced from a function defined alongside the
           select.
        2. The address is a dotted child whose parent is itself a scoped
           placeholder (a function/rowset parameter materialized into the
           env as ``UndefinedConceptFull``, or a previously staged
           placeholder). Mirrors v1 lazy lookup of ``param.field`` chains.

        Both paths stage the placeholder via
        :meth:`SemanticState.stage_placeholder` so it is visible to v1
        helpers through the env overlay but never commits.
        """
        if self._symbol_table is not None:
            for candidate in self._candidate_addresses(address):
                if self._symbol_table.lookup(candidate) is not None:
                    return self._make_scoped_placeholder(candidate)
        if "." not in address:
            return None
        for candidate in self._candidate_addresses(address):
            if "." not in candidate:
                continue
            parent_address, _ = candidate.rsplit(".", 1)
            parent = self._existing_concept(parent_address)
            if parent is None and self._symbol_table is not None:
                for parent_candidate in self._candidate_addresses(parent_address):
                    if self._symbol_table.lookup(parent_candidate) is not None:
                        parent = self._make_scoped_placeholder(parent_candidate)
                        break
            if isinstance(parent, UndefinedConceptFull):
                return self._make_scoped_placeholder(candidate)
        return None

    def _deferred_placeholder(self, address: str) -> Concept | None:
        aliases = self._state.deferred_import_aliases
        if not aliases:
            return None
        for candidate in self._candidate_addresses(address):
            namespace, _, name = candidate.rpartition(".")
            if not namespace or not name:
                continue
            root_alias = namespace.split(".", 1)[0]
            if root_alias not in aliases:
                continue
            cached = self._state.pending_lookup(candidate)
            if cached is not None:
                return cached
            placeholder = UndefinedConceptFull(
                name=name,
                namespace=namespace,
                datatype=DataType.UNKNOWN,
                purpose=Purpose.UNKNOWN,
            )
            self._state.add(placeholder, ConceptUpdateKind.VIRTUAL_HELPER)
            return placeholder
        return None

    def _resolve_property_sibling(self, address: str) -> Concept | None:
        """Resolve `key_name.prop_name` shorthand to a sibling property concept.

        `property x.part_1 string;` declares a concept at `local.part_1` with
        ``keys={'local.x'}`` — the syntactic parent address ``x.part_1`` never
        exists as a standalone key. References to ``x.part_1`` in expressions
        (e.g. ``concat(x.part_1, x.part_2)``) should resolve to that sibling
        property. v1 quietly returned an UndefinedConcept via ``fail_on_missing``
        and let the reference stay dangling; v2 resolves it strictly by
        verifying the key relationship and returning the real concept.
        """
        if "." not in address:
            return None
        for candidate in self._candidate_addresses(address):
            if "." not in candidate:
                continue
            key_name, suffix = candidate.rsplit(".", 1)
            key_concept = self._existing_concept(key_name)
            if key_concept is None:
                continue
            namespace = key_concept.namespace or DEFAULT_NAMESPACE
            sibling_addr = f"{namespace}.{suffix}"
            sibling = self._existing_concept(sibling_addr)
            if sibling is None:
                continue
            sibling_keys = sibling.keys
            if sibling_keys and key_concept.address in sibling_keys:
                return sibling
        return None

    def _rowset_output_addresses(self, prefix: str) -> set[str]:
        """Every known rowset-output address under ``prefix`` — committed,
        pending, AND symbol-table forward refs. The forward refs matter because
        `auto`/`def` bodies hydrate in the concept phase (right after BIND),
        before the rowset statement stages any concept; COLLECT_SYMBOLS has
        already declared the full output paths by then."""
        addrs = {a for a in self._env.concepts.data if a.startswith(prefix)}
        addrs.update(
            a for a, _ in self._state.pending_concepts() if a.startswith(prefix)
        )
        if self._symbol_table is not None:
            addrs.update(
                a
                for a in self._symbol_table.visible_addresses()
                if a.startswith(prefix)
            )
        return addrs

    def _resolve_rowset_suffix(self, address: str) -> Concept | None:
        """Resolve a rowset leaf-shorthand (`rs.col`) to the full output path
        (`rs.a.b.col`) when exactly one rowset output under namespace `rs`
        matches as an ordered dotted subsequence. Two matches -> ambiguity
        error; zero -> None. Gated to rowset namespaces (registered at
        COLLECT_SYMBOLS) so it never collapses import paths. When the single
        match is not yet a real concept (still a symbol-table forward ref),
        return a scoped placeholder the rowset's later commit displaces."""
        rowset_namespaces = self._env.concepts.rowset_namespaces
        for candidate in self._candidate_addresses(address):
            q_segs = candidate.split(".")
            if len(q_segs) < 2 or q_segs[0] not in rowset_namespaces:
                continue
            prefix = q_segs[0] + "."
            matches = [
                addr
                for addr in self._rowset_output_addresses(prefix)
                if addr != candidate and _is_subsequence(q_segs, addr.split("."))
            ]
            if len(matches) == 1:
                target = matches[0]
                existing = self._existing_concept(target)
                if existing is not None and not isinstance(
                    existing, UndefinedConceptFull
                ):
                    return existing
                return self._make_scoped_placeholder(target)
            if len(matches) > 1:
                raise UndefinedConceptException(
                    f"Ambiguous reference {address!r}: matches {sorted(matches)}. "
                    "Qualify the full path to disambiguate.",
                    sorted(matches),
                )
        return None

    def require(self, address: str) -> Concept:
        existing = self._existing_concept(address)
        if existing is not None:
            return existing
        for candidate in self._candidate_addresses(address):
            derived = self._auto_derive(candidate)
            if derived is not None:
                return derived
        sibling = self._resolve_property_sibling(address)
        if sibling is not None:
            return sibling
        rowset_suffix = self._resolve_rowset_suffix(address)
        if rowset_suffix is not None:
            return rowset_suffix
        scoped = self._scoped_placeholder(address)
        if scoped is not None:
            return scoped
        deferred = self._deferred_placeholder(address)
        if deferred is not None:
            return deferred
        try:
            return self._env.concepts[address]  # type: ignore[return-value]
        except UndefinedConceptException:
            # The dict-level raise only sees concepts committed to
            # ``environment.concepts``; named-statement (`rowset` / `with … as`)
            # outputs staged this parse aren't committed until end-of-parse, so
            # surface them via ``extra_keys`` — mirrors the select-finalize
            # raise sites (`_staged_addresses`).
            self._raise_undefined(address)

    def _raise_undefined(self, address: str) -> Never:
        staged = [c.address for c in self.values()]
        matches = self._env.concepts._find_similar_concepts(address, extra_keys=staged)
        message = f"Undefined concept: {address}."
        if matches:
            message += f" Suggestions: {matches}"
        raise UndefinedConceptException(message, matches)

    def get(self, address: str) -> Concept | None:
        existing = self._existing_concept(address)
        if existing is not None:
            return existing
        for candidate in self._candidate_addresses(address):
            derived = self._auto_derive(candidate)
            if derived is not None:
                return derived
        sibling = self._resolve_property_sibling(address)
        if sibling is not None:
            return sibling
        rowset_suffix = self._resolve_rowset_suffix(address)
        if rowset_suffix is not None:
            return rowset_suffix
        scoped = self._scoped_placeholder(address)
        if scoped is not None:
            return scoped
        return self._deferred_placeholder(address)

    def contains(self, address: str) -> bool:
        if self._existing_concept(address) is not None:
            return True
        if self._resolve_property_sibling(address) is not None:
            return True
        if self._scoped_placeholder(address) is not None:
            return True
        return self._deferred_placeholder(address) is not None

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
