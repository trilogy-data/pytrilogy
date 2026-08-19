"""Per-query treatment of ``~`` (partial) key bindings before discovery.

A ``~`` binding licenses domain extension: unmatched members of that key's
dimension enter the result once, carrying their own attributes, with every
concept outside the key's functional closure NULL. Two consequences are handled
here, at one seam, so every downstream consumer (source search, condition
routing, join planning, optimizers) sees a single consistent judgment:

1. ``heal_pinned_partials`` — when the statement WHERE proves non-null a bound
   concept OUTSIDE a partial key's closure, every extension row that key could
   license is filtered out (the concept is manufactured-NULL on those rows), so
   the binding is complete *for this query*. Dropping the modifier up front
   lets the fact anchor the plan: INNER star joins instead of the
   anchor-LEFT + coalesce extension scaffolding that the same query otherwise
   plans and then filters.

2. ``validate_partial_bridges`` — when the statement needs two keys related and
   every datasource relating them binds one of the keys partially (with the
   extension NOT killed by the WHERE), the pairing is only defined for rows
   where the relation exists; unmatched members of either side have no
   well-defined counterpart. Rather than guessing, raise
   ``UnconstrainedPartialBridgeException`` telling the author exactly which
   filter pins the population.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import Derivation, Granularity, Modifier, Purpose
from trilogy.core.exceptions import UnconstrainedPartialBridgeException
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import condition_proves_non_null
from trilogy.core.processing.v4_helper.functional_dependency import build_fd_closure


def _spellings(concept: BuildConcept) -> set[str]:
    return {concept.address, concept.canonical_address, *concept.pseudonyms}


def _bound_spellings(datasources: Iterable[BuildDatasource]) -> set[str]:
    out: set[str] = set()
    for ds in datasources:
        for column in ds.columns:
            out |= _spellings(column.concept)
    return out


def _structural_partial(ds: BuildDatasource, column: BuildColumnAssignment) -> bool:
    """True for a column-level ``~``, the only mark that licenses extension.

    A table-level partial stamp (``partial datasource ... complete where``) is
    a row-subset contract the union machinery completes across siblings, and
    still relates its keys — treating it as an extension license breaks that
    assembly.
    """
    return (
        Modifier.PARTIAL in column.modifiers
        and column.concept.address in ds.column_level_partial_addresses
    )


def _build_datasources(environment: BuildEnvironment) -> list[BuildDatasource]:
    return [
        ds for ds in environment.datasources.values() if isinstance(ds, BuildDatasource)
    ]


def _proven_bound(
    conditions: BuildWhereClause | None,
    datasources: list[BuildDatasource],
) -> set[str]:
    """WHERE-proven non-null addresses that are physically bound somewhere.

    Restricting proofs to bound columns guards against tautologies: a derived
    ``coalesce(x, 5) is not null`` proves the derivation's own address non-null
    while saying nothing about any row's origin, so it must not count as
    evidence that extension rows are filtered out.
    """
    if conditions is None:
        return set()
    proven = condition_proves_non_null(conditions.conditional)
    if not proven:
        return set()
    return proven & _bound_spellings(datasources)


def _extension_killed(
    environment: BuildEnvironment,
    key: BuildConcept,
    proven_bound: set[str],
    reachable: set[str],
) -> bool:
    """True when the WHERE filters out every extension row ``key`` licenses.

    An extension row carries values only for ``key``'s own functional closure —
    everything else on it is manufactured NULL. A proven-non-null bound concept
    outside that closure therefore kills the row. ``reachable`` limits killers
    to concepts related to the key's own model component: a concept from a
    disconnected subgraph attaches to the result via a cross-join gate and is
    non-null on extension rows too, so it proves nothing.
    """
    candidates = {p for p in proven_bound if p in reachable}
    if not candidates:
        return False
    closure = build_fd_closure(environment, _spellings(key), include_empty_grain=True)
    return any(p not in closure for p in candidates)


def _pair_anchored(
    key_spellings: set[str], ds: BuildDatasource, datasources: list[BuildDatasource]
) -> bool:
    """True when a sibling row-source carries this key inside a LARGER grain.

    Such a sibling supplies key combinations beyond ``ds``'s subset (the
    store_sales anchor for store_returns' ``~`` grain keys), so even a pin that
    kills dimension extensions does not shrink the population to ``ds``'s own
    rows — the binding must stay partial and the sibling-stitch machinery owns
    the merge.
    """
    for other in datasources:
        if other.identifier == ds.identifier:
            continue
        grain = set(other.grain.components)
        if grain & key_spellings and grain - key_spellings:
            return True
    return False


def _component_reach(
    ds: BuildDatasource, datasources: list[BuildDatasource]
) -> set[str]:
    """All concept spellings connected to ``ds`` through shared bindings."""
    reach = _bound_spellings([ds])
    changed = True
    remaining = [d for d in datasources if d.identifier != ds.identifier]
    while changed:
        changed = False
        still: list[BuildDatasource] = []
        for other in remaining:
            other_spellings = _bound_spellings([other])
            if other_spellings & reach:
                reach |= other_spellings
                changed = True
            else:
                still.append(other)
        remaining = still
    return reach


def _reach(
    ds: BuildDatasource, datasources: list[BuildDatasource], cache: dict[str, set[str]]
) -> set[str]:
    reach = cache.get(ds.identifier)
    if reach is None:
        reach = _component_reach(ds, datasources)
        cache[ds.identifier] = reach
    return reach


def heal_pinned_partials(
    environment: BuildEnvironment, conditions: BuildWhereClause | None
) -> None:
    """Drop ``~`` from bindings whose licensed extensions this WHERE kills.

    Copy-on-write: affected datasources are replaced in the environment's (per-
    statement) mapping; the shared build-cache objects are never mutated.
    """
    datasources = _build_datasources(environment)
    partial_hosts = [
        ds for ds in datasources if any(_structural_partial(ds, c) for c in ds.columns)
    ]
    if not partial_hosts:
        return
    proven_bound = _proven_bound(conditions, datasources)
    if not proven_bound:
        return
    reach_cache: dict[str, set[str]] = {}
    replacements: dict[str, BuildDatasource] = {}
    for ds in partial_hosts:
        reach = _reach(ds, datasources, reach_cache)
        healed: set[str] = set()
        for column in ds.columns:
            if not _structural_partial(ds, column):
                continue
            key = column.concept
            if _pair_anchored(_spellings(key), ds, datasources):
                continue
            if _extension_killed(environment, key, proven_bound, reach):
                healed.add(key.address)
        if not healed:
            continue
        new_columns = [
            (
                BuildColumnAssignment(
                    alias=c.alias,
                    concept=c.concept,
                    modifiers=c.modifiers - {Modifier.PARTIAL},
                    origin_address=c.origin_address,
                )
                if c.concept.address in healed and Modifier.PARTIAL in c.modifiers
                else c
            )
            for c in ds.columns
        ]
        replacements[ds.identifier] = dataclasses.replace(
            ds,
            columns=new_columns,
            column_level_partial_addresses=set(ds.column_level_partial_addresses)
            - healed,
        )
    if not replacements:
        return
    for name, existing in list(environment.datasources.items()):
        if (
            isinstance(existing, BuildDatasource)
            and existing.identifier in replacements
        ):
            environment.datasources[name] = replacements[existing.identifier]


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        parent = self.parent.setdefault(item, item)
        if parent == item:
            return item
        root = self.find(parent)
        self.parent[item] = root
        return root

    def union(self, a: str, b: str) -> None:
        self.parent[self.find(a)] = self.find(b)

    def connected(self, a: str, b: str) -> bool:
        return self.find(a) == self.find(b)


def _canonical_key(environment: BuildEnvironment, address: str) -> str:
    concept = environment.concepts.get(address)
    return concept.canonical_address if concept is not None else address


def _home_keys(environment: BuildEnvironment, concept: BuildConcept) -> set[str]:
    """The key domains a ROW-level reference to ``concept`` obligates the query
    to relate. Aggregates collapse their internals and derived concepts carry
    no reliable home, so both contribute nothing — under-reporting only ever
    means falling back to today's behavior."""
    if concept.is_aggregate or concept.granularity == Granularity.SINGLE_ROW:
        return set()
    if concept.derivation != Derivation.ROOT:
        return set()
    if concept.purpose == Purpose.KEY:
        return {concept.canonical_address}
    if concept.keys:
        return {_canonical_key(environment, k) for k in concept.keys}
    return set()


def _display(address: str) -> str:
    prefix = f"{DEFAULT_NAMESPACE}."
    return address.removeprefix(prefix)


def validate_partial_bridges(
    environment: BuildEnvironment, statement: BuildSelectLineage
) -> None:
    """Reject key spans whose only relating datasources are UNSAFE bridges.

    Runs AFTER ``heal_pinned_partials``: a binding healed by the WHERE is
    complete here, and a still-partial binding also counts as complete when the
    WHERE kills its extensions (the sibling-anchored case heals nothing but is
    population-safe under the pin).

    A datasource relates its bound keys SAFELY when:

    - at most ONE of the keys the query asks about is a live (un-killed)
      structural ``~`` there — a single extension family is well-defined (the
      unmatched members appear once with NULLs elsewhere); or
    - its row identity anchors the output: every grain component is bound
      complete AND selected, so each result row is a fact row or one dimension's
      extension row (``select store_id, product_id, order_id`` through an
      orders fact binding both FKs ``~``).

    What remains — a multi-``~`` bridge whose row identity is absent from the
    output or IS the partial keys — is the customer x product x partial-sales
    shape: the pairing exists only where the bridge has a row, and unmatched
    members of either side have no defined counterpart.
    """
    datasources = _build_datasources(environment)
    referenced: list[BuildConcept] = list(statement.output_components)
    if statement.where_clause is not None:
        referenced.extend(statement.where_clause.row_arguments)
    required: set[str] = set()
    for concept in referenced:
        required |= _home_keys(environment, concept)
    if len(required) < 2:
        return
    proven_bound = _proven_bound(statement.where_clause, datasources)

    complete = _UnionFind()
    full = _UnionFind()
    partial_bridges: dict[str, set[str]] = {}
    reach_cache: dict[str, set[str]] = {}
    for ds in datasources:
        bound_keys: list[tuple[str, bool]] = []
        for column in ds.columns:
            if column.concept.purpose != Purpose.KEY:
                continue
            canonical = _canonical_key(environment, column.concept.address)
            is_partial = _structural_partial(ds, column)
            if is_partial and proven_bound:
                reach = _reach(ds, datasources, reach_cache)
                if _extension_killed(environment, column.concept, proven_bound, reach):
                    is_partial = False
            bound_keys.append((canonical, is_partial))
        for canonical, _ in bound_keys[1:]:
            full.union(bound_keys[0][0], canonical)
        partial_keys = {c for c, is_partial in bound_keys if is_partial}
        grain_canonical = {
            _canonical_key(environment, component) for component in ds.grain.components
        }
        anchored = (
            bool(grain_canonical)
            and grain_canonical.isdisjoint(partial_keys)
            and grain_canonical <= required
        )
        # Query-scoped family count: a partial key the output never asks about
        # licenses no extension rows here, so only required keys count toward
        # the two-family danger (orders binding ~customer AND ~product is a
        # safe bridge for a customer x order_date span).
        if len(partial_keys & required) <= 1 or anchored:
            all_keys = [c for c, _ in bound_keys]
            for canonical in all_keys[1:]:
                complete.union(all_keys[0], canonical)
            continue
        complete_keys = [c for c, is_partial in bound_keys if not is_partial]
        for canonical in complete_keys[1:]:
            complete.union(complete_keys[0], canonical)
        partial_bridges[ds.identifier] = partial_keys

    offending: list[tuple[str, str]] = []
    ordered = sorted(required)
    for i, left in enumerate(ordered):
        for right in ordered[i + 1 :]:
            if complete.connected(left, right):
                continue
            if full.connected(left, right):
                offending.append((left, right))
    if not offending:
        return

    involved_keys = sorted({key for pair in offending for key in pair})
    bridges = sorted(
        name
        for name, keys in partial_bridges.items()
        if any(full.connected(k, involved_keys[0]) for k in keys)
    )
    partial_marks = sorted(
        {
            _display(k)
            for name in bridges
            for k in partial_bridges[name]
            if k in involved_keys or any(full.connected(k, i) for i in involved_keys)
        }
    )
    display_keys = [_display(k) for k in involved_keys]
    suggestion = "where " + " and ".join(f"{k} is not null" for k in display_keys)
    message = (
        f"This query needs {' and '.join(display_keys)} related, but every "
        f"datasource relating them ({', '.join(bridges)}) covers only a subset "
        f"of that relationship (partial `~` bindings on: {', '.join(partial_marks)}). "
        "Members of one side with no matching row there have no defined "
        "counterpart on the other side, so the combined result cannot be "
        "generated safely. Add a filter that pins the population to rows where "
        f"the relationship exists, for example:\n\n  {suggestion}\n\n"
        "or filter on any attribute the relating datasource provides. To "
        "include unmatched members, query each side separately."
    )
    raise UnconstrainedPartialBridgeException(
        message,
        keys=display_keys,
        datasources=bridges,
        suggestion=suggestion,
    )
