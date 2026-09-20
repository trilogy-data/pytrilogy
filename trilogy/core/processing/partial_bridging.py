"""Per-query treatment of ``~`` (partial) key bindings before discovery.

A ``~`` binding licenses domain extension: unmatched members of that key's
dimension enter the result once, carrying their own attributes, with every
concept outside the key's functional closure NULL.

``heal_pinned_partials``: when the statement WHERE proves non-null a bound
concept OUTSIDE a partial key's closure, every extension row that key could
license is filtered out (the concept is manufactured-NULL on those rows), so
the binding is complete for this query. Dropping the modifier up front lets
the fact anchor the plan with INNER star joins instead of extension
scaffolding that is then filtered away. Running at one seam
(``get_query_node``) keeps every downstream consumer on one judgment.

``drop_excluded_partials``: a ``complete where`` source whose partition
predicate is mutually exclusive with the statement's row gate cannot contribute
a row, so it is hidden from discovery. Left visible it still counts as a
binding: a bare key it binds is planned as a scan instead of through its
``merge`` origin, and a union over the sibling partition is deemed complete and
then filtered to nothing. The enum values the gate rules out are recorded on
the environment so the surviving arms are still proven complete over the
domain that remains.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable

from trilogy.core.enums import Modifier
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import EnumType
from trilogy.core.processing.condition_utility import (
    condition_proves_non_null,
    conditions_mutually_exclusive,
    gate_allowed_values,
)
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
    still relates its keys; treating it as an extension license breaks that
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
    killers: set[str],
) -> bool:
    """True when the WHERE filters out every extension row ``key`` licenses.

    An extension row carries values only for ``key``'s own functional closure;
    everything else on it is manufactured NULL. A proven-non-null bound concept
    outside that closure therefore kills the row.
    """
    closure = build_fd_closure(environment, _spellings(key), include_empty_grain=True)
    return any(p not in closure for p in killers)


def _partition_disjoint(a: BuildDatasource, b: BuildDatasource) -> bool:
    """``complete where`` partitions whose predicates exclude each other never
    share a row, so neither can anchor the other's keys."""
    return (
        a.non_partial_for is not None
        and b.non_partial_for is not None
        and conditions_mutually_exclusive(
            a.non_partial_for.conditional, b.non_partial_for.conditional
        )
    )


def _pair_anchors(
    key_spellings: set[str], ds: BuildDatasource, datasources: list[BuildDatasource]
) -> list[BuildDatasource]:
    """Sibling row-sources carrying this key inside a LARGER grain.

    Such a sibling supplies key combinations beyond ``ds``'s subset, so a pin
    that kills dimension extensions does not by itself shrink the population
    to ``ds``'s own rows; unless the anchors are dispensable for the statement
    the binding stays partial and the sibling-stitch machinery owns the merge.
    """
    anchors: list[BuildDatasource] = []
    for other in datasources:
        if other.identifier == ds.identifier or _partition_disjoint(ds, other):
            continue
        grain = set(other.grain.components)
        if grain & key_spellings and grain - key_spellings:
            anchors.append(other)
    return anchors


def _lookup_supply(
    anchor: BuildDatasource, datasources: list[BuildDatasource]
) -> set[str]:
    """Spellings an ``anchor`` row can carry a value for: its own bindings plus
    every datasource reachable by keyed lookup on what it already carries.

    The FD closure is the wrong tool here: a sibling at the SAME grain binding
    its keys ``~`` puts its columns in the closure, yet may hold no row for the
    anchor's key, so the lookup walk stops at partial key bindings. Sources it
    does enter over-approximate (a nullable FK may miss), which is the safe
    direction: a killer counted as suppliable only blocks healing.
    """
    supply = _bound_spellings([anchor])
    remaining = [d for d in datasources if d.identifier != anchor.identifier]
    changed = True
    while changed:
        changed = False
        still: list[BuildDatasource] = []
        for d in remaining:
            grain = set(d.grain.components)
            partial_key = any(
                _structural_partial(d, c) and c.concept.address in grain
                for c in d.columns
            )
            if grain <= supply and not partial_key:
                supply |= _bound_spellings([d])
                changed = True
            else:
                still.append(d)
        remaining = still
    return supply


def _anchors_dispensable(
    ds: BuildDatasource,
    anchors: list[BuildDatasource],
    killers: set[str],
    referenced_bound: set[str],
    datasources: list[BuildDatasource],
) -> bool:
    """True when the WHERE filters out every anchor-only row and the statement
    can be answered from ``ds``'s own rows without any anchor.

    An anchor's rows carry values only for what they bind or can look up and
    are NULL elsewhere, so a proven-non-null concept outside that supply kills
    them exactly as it kills a dimension extension. The second guard is
    load-bearing: were the statement to reference a concept ``ds`` can only
    reach through an anchor (a sales measure, or a dimension hung off the
    sale's own key), the healed key would license an INNER merge with the
    anchor that drops the fact's own unmatched rows (a return whose sale is
    absent). Partition-disjoint siblings never serve ``ds``'s rows, so their
    bindings do not count either.
    """
    for anchor in anchors:
        if killers <= _lookup_supply(anchor, datasources):
            return False
    anchor_ids = {a.identifier for a in anchors}
    usable = [
        d
        for d in datasources
        if d.identifier not in anchor_ids and not _partition_disjoint(ds, d)
    ]
    return referenced_bound <= _lookup_supply(ds, usable)


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
    referenced_bound = (environment.statement_authored_addresses or set()) & (
        _bound_spellings(datasources)
    )
    reach_cache: dict[str, set[str]] = {}
    replacements: dict[str, BuildDatasource] = {}
    for ds in partial_hosts:
        # A killer must be related to the key's own model component: a concept
        # from a disconnected subgraph attaches via a cross-join gate and is
        # non-null on extension rows too, so it proves nothing.
        reach = _reach(ds, datasources, reach_cache)
        killers = proven_bound & reach
        if not killers:
            continue
        # References outside the component (a membership set built from a
        # separately imported dimension) are sourced by their own subquery,
        # never through an anchor.
        component_refs = referenced_bound & reach
        healed: set[str] = set()
        for column in ds.columns:
            if not _structural_partial(ds, column):
                continue
            key = column.concept
            anchors = _pair_anchors(_spellings(key), ds, datasources)
            if anchors and not _anchors_dispensable(
                ds, anchors, killers, component_refs, datasources
            ):
                continue
            if _extension_killed(environment, key, killers):
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


def _gate_excluded_enum_values(
    environment: BuildEnvironment, stage: BuildWhereClause
) -> dict[str, frozenset[str]]:
    """Enum discriminator values the gate's literal atoms rule out, keyed by
    address and canonical address."""
    out: dict[str, frozenset[str]] = {}
    for address, allowed in gate_allowed_values(stage.conditional).items():
        concept = environment.concepts.get(address)
        if concept is None or not isinstance(concept.datatype, EnumType):
            continue
        gone = frozenset(str(v) for v in concept.datatype.values) - {
            str(v) for v in allowed
        }
        if gone:
            out[concept.address] = gone
            out[concept.canonical_address] = gone
    return out


def drop_excluded_partials(
    environment: BuildEnvironment, stage: BuildWhereClause | None
) -> None:
    """Hide every ``complete where`` source the statement's row bound rules out.

    ``stage`` is ``universal_row_bound``: the predicate every row the statement
    reads satisfies, so a source whose partition predicate contradicts it holds
    no usable row. Deciding which stages that bound may draw on belongs to the
    staging rules, not here; None means the statement has no such bound and
    nothing is hidden. Removal is from the per-statement mapping only; shared
    build-cache objects are untouched.
    """
    if stage is None:
        return
    environment.excluded_enum_values = _gate_excluded_enum_values(environment, stage)
    excluded = [
        name
        for name, ds in environment.datasources.items()
        if isinstance(ds, BuildDatasource)
        and ds.non_partial_for is not None
        and conditions_mutually_exclusive(
            stage.conditional, ds.non_partial_for.conditional
        )
    ]
    for name in excluded:
        del environment.datasources[name]
