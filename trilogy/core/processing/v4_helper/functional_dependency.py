from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import partial
from typing import Generic, TypeVar
from weakref import ReferenceType, ref

from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment

from .models import ConceptAttrs

T = TypeVar("T")


class EnvCache(dict[int, tuple[ReferenceType[BuildEnvironment], T]], Generic[T]):
    """id(environment) -> (weak handle, value). A BuildEnvironment's concepts
    and datasources are fixed at construction (later writes go to the
    ReferenceGraph, the authored Environment or a StrategyNode), so a value
    can never go stale, only be discarded with its environment. The weak
    handle makes the identity check exact: a recycled id cannot false-hit,
    because a dead referent is never the live environment."""

    def lookup(self, environment: BuildEnvironment) -> T | None:
        cached = self.get(id(environment))
        if cached is not None and cached[0]() is environment:
            return cached[1]
        return None

    def store(self, environment: BuildEnvironment, value: T) -> T:
        key = id(environment)
        self[key] = (ref(environment, partial(self._evict, key)), value)
        return value

    def _evict(self, key: int, _dead: ReferenceType) -> None:
        self.pop(key, None)


def _attrs_for_address(
    concept_attrs: dict[str, ConceptAttrs], address: str
) -> Iterable[ConceptAttrs]:
    for attrs in concept_attrs.values():
        if attrs.address == address:
            yield attrs


def concept_attr_fd_closure(
    concept_attrs: dict[str, ConceptAttrs],
    determinants: Iterable[str],
    *,
    include_empty_grain: bool = True,
) -> frozenset[str]:
    closure = set(determinants)
    changed = True
    while changed:
        changed = False
        for attrs in concept_attrs.values():
            if attrs.address in closure:
                continue
            if not attrs.grain_components and include_empty_grain:
                closure.add(attrs.address)
                changed = True
                continue
            # Declared keys are an FD even when the concept carries no grain,
            # mirroring build_fd_closure, unless they only determine the value
            # conditionally, which is not an FD at all (see
            # `ConceptAttrs.keys_are_conditional_fd`).
            if (attrs.grain_components and attrs.grain_components <= closure) or (
                bool(attrs.keys)
                and not attrs.keys_are_conditional_fd
                and attrs.keys <= closure
            ):
                closure.add(attrs.address)
                changed = True
    return frozenset(closure)


def concept_attr_fd_determines(
    concept_attrs: dict[str, ConceptAttrs],
    determinants: Iterable[str],
    address: str,
    *,
    include_empty_grain: bool = True,
) -> bool:
    if address in determinants:
        return True
    if not any(_attrs_for_address(concept_attrs, address)):
        return False
    return address in concept_attr_fd_closure(
        concept_attrs,
        determinants,
        include_empty_grain=include_empty_grain,
    )


def _build_fd_concepts(environment: BuildEnvironment) -> Iterator[BuildConcept]:
    yield from environment.concepts.values()
    for datasource in environment.datasources.values():
        yield from datasource.output_concepts


@dataclass(frozen=True)
class _FDFacts:
    """The FD-relevant attributes of one environment, as plain data.

    `build_fd_closure` is a fixpoint over a set of addresses, and every
    attribute it tests is immutable for the life of the environment, so they
    are read off the BuildConcepts once rather than per iteration
    (`equivalent_addresses` and the keys set both allocate per read)."""

    # (address, grain components, keys) for concepts AND datasource columns.
    # Deduplicated on the whole triple, never on address alone: a datasource
    # column can carry a different grain than the environment's concept of the
    # same address, and each spelling is its own FD.
    rows: tuple[tuple[str, frozenset[str], frozenset[str]], ...]
    # Address -> its equivalents, filled lazily for addresses that are not
    # environment keys (`concepts.get` resolves namespace-prefixed spellings).
    equivalents: dict[str, frozenset[str]]
    # (determinants, include_empty_grain) -> closure. The closure is a pure
    # function of these facts, so memoizing it here needs no soundness argument
    # beyond the one `_FACTS_CACHE` already makes, and eviction rides on the
    # facts entry.
    closures: dict[tuple[frozenset[str], bool], frozenset[str]]
    # address -> the environment keys it adds (as the key's concept address
    # or an equivalent: a key can differ from its concept's own address, and
    # the closure carries both) and the rows whose grain or keys read it
    entry_triggers: dict[str, list[str]]
    row_triggers: dict[str, list[int]]

    def equivalents_for(
        self, environment: BuildEnvironment, address: str
    ) -> frozenset[str]:
        # The environment is passed in rather than held: the table outlives the
        # call (see `_FACTS_CACHE`), and a field here would pin every
        # environment it was ever built for.
        cached = self.equivalents.get(address)
        if cached is None:
            concept = environment.concepts.get(address)
            cached = (
                frozenset(concept.equivalent_addresses)
                if concept is not None
                else frozenset()
            )
            self.equivalents[address] = cached
        return cached


_FACTS_CACHE: EnvCache[_FDFacts] = EnvCache()


def _fd_facts(environment: BuildEnvironment) -> _FDFacts:
    cached = _FACTS_CACHE.lookup(environment)
    if cached is not None:
        return cached
    entries: list[tuple[str, str, frozenset[str]]] = []
    equivalents: dict[str, frozenset[str]] = {}
    for key, concept in environment.concepts.items():
        equivalent = frozenset(concept.equivalent_addresses)
        entries.append((key, concept.address, equivalent))
        equivalents[key] = equivalent
    rows: list[tuple[str, frozenset[str], frozenset[str]]] = []
    seen: set[tuple[str, frozenset[str], frozenset[str]]] = set()
    for concept in _build_fd_concepts(environment):
        row = (
            concept.address,
            frozenset(concept.grain.components) if concept.grain else frozenset(),
            frozenset(concept.keys or ()),
        )
        if row in seen:
            continue
        seen.add(row)
        rows.append(row)
    entry_triggers: dict[str, list[str]] = {}
    for key, own, equivalent in entries:
        for address in {own, *equivalent}:
            entry_triggers.setdefault(address, []).append(key)
    row_triggers: dict[str, list[int]] = {}
    for i, (_, grain, keys) in enumerate(rows):
        for address in grain | keys:
            row_triggers.setdefault(address, []).append(i)
    return _FACTS_CACHE.store(
        environment,
        _FDFacts(
            rows=tuple(rows),
            equivalents=equivalents,
            closures={},
            entry_triggers=entry_triggers,
            row_triggers=row_triggers,
        ),
    )


def build_fd_closure(
    environment: BuildEnvironment,
    determinants: Iterable[str],
    *,
    include_empty_grain: bool = True,
) -> frozenset[str]:
    facts = _fd_facts(environment)
    seed = frozenset(determinants)
    memo_key = (seed, include_empty_grain)
    memoized = facts.closures.get(memo_key)
    if memoized is not None:
        return memoized
    # A worklist over the rules each new address can fire: the closure is a
    # least fixpoint, so the order rules fire in cannot change it.
    closure: set[str] = set()
    pending = list(seed)
    if include_empty_grain:
        pending.extend(address for address, grain, _ in facts.rows if not grain)
    while pending:
        address = pending.pop()
        if address in closure:
            continue
        closure.add(address)
        pending.extend(facts.equivalents_for(environment, address))
        pending.extend(facts.entry_triggers.get(address, ()))
        for i in facts.row_triggers.get(address, ()):
            target, grain, keys = facts.rows[i]
            if target in closure:
                continue
            # Declared keys are an FD even when the concept carries no grain
            # (a filter virtual with keys and an empty grain).
            if (grain and grain <= closure) or (keys and keys <= closure):
                pending.append(target)
    result = frozenset(closure)
    facts.closures[memo_key] = result
    return result


def build_fd_determines(
    environment: BuildEnvironment,
    determinants: Iterable[str],
    address: str,
    *,
    include_empty_grain: bool = True,
) -> bool:
    return address in build_fd_closure(
        environment,
        determinants,
        include_empty_grain=include_empty_grain,
    )


def minimize_build_grain(
    environment: BuildEnvironment,
    grain: Iterable[str],
) -> frozenset[str]:
    minimized = set(grain)
    changed = True
    while changed:
        changed = False
        for address in sorted(minimized):
            determinants = minimized - {address}
            if not determinants:
                continue
            if build_fd_determines(
                environment,
                determinants,
                address,
                include_empty_grain=False,
            ):
                minimized.remove(address)
                changed = True
                break
    return frozenset(minimized)
