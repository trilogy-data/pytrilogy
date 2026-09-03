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
from trilogy.core.processing.v4_helper.staged_where import stage_computes_cross_row


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
    proven_bound: set[str],
    reachable: set[str],
) -> bool:
    """True when the WHERE filters out every extension row ``key`` licenses.

    An extension row carries values only for ``key``'s own functional closure;
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

    Such a sibling supplies key combinations beyond ``ds``'s subset, so even a
    pin that kills dimension extensions does not shrink the population to
    ``ds``'s own rows; the binding stays partial and the sibling-stitch
    machinery owns the merge.
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
    """Hide every ``complete where`` source the first WHERE stage rules out.

    ``stage`` is the statement's stage-1 row gate: every row the statement
    reads passes it, so a source whose partition predicate contradicts it holds
    no usable row. A stage that itself computes an aggregate or window sees the
    full population, so nothing is hidden for it. Removal is from the
    per-statement mapping only; shared build-cache objects are untouched.
    """
    if stage is None or stage_computes_cross_row(stage):
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
