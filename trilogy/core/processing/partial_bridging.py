"""Per-query treatment of ``~`` (partial) key bindings before discovery.

A ``~`` binding licenses domain extension: unmatched members of that key's
dimension enter the result once, carrying their own attributes, with every
concept outside the key's functional closure NULL.

``decide_heal``: when the statement WHERE empties every kind of row a ``~``
binding's source has no match for, the binding is complete for this query.
WHICH rows those are is the keyspace's answer (``Keyspace.binding_is_complete``,
over the statement's bindings as authored and the WHERE's non-null proofs);
whether dropping the ``~`` is also safe for every other merge it would license
is decided here (the anchor guards). Dropping the modifier up front lets the
fact anchor the plan with INNER star joins instead of extension scaffolding
that is then filtered away. It is decided once per PLAN, on the plan's own
outputs, WHERE and references (``statement_scope.generate_scope_graph``): the
statement at ``get_query_node`` and every nested select (a rowset body, a union
arm), so every downstream consumer of that plan reads one judgment.

``decide_exclusion``: a ``complete where`` source whose partition predicate is
mutually exclusive with the statement's row gate cannot contribute a row, so
it is hidden from discovery. Left visible it still counts as a binding: a bare
key it binds is planned as a scan instead of through its ``merge`` origin, and
a union over the sibling partition is deemed complete and then filtered to
nothing. The enum values the gate rules out (``gate_excluded_enum_values``)
are recorded on the environment so the surviving arms are still proven
complete over the domain that remains.

Both are pure over the datasource list they are given; applying them is the
caller's.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Sequence
from functools import cached_property

from trilogy.core.enums import Derivation, Modifier
from trilogy.core.graph_models import ScopeDatasources
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import EnumType
from trilogy.core.models.keyspace import Keyspace
from trilogy.core.processing.condition_utility import (
    conditions_mutually_exclusive,
    gate_allowed_values,
)
from trilogy.core.processing.v4_helper.concept_graph import build_concept_graph
from trilogy.core.processing.v4_helper.keyspace import build_keyspace, null_rejected


def _spellings(concept: BuildConcept) -> set[str]:
    return {concept.address, concept.canonical_address, *concept.pseudonyms}


def _bound_spellings(datasources: Iterable[BuildDatasource]) -> set[str]:
    out: set[str] = set()
    for ds in datasources:
        for column in ds.columns:
            out |= _spellings(column.concept)
    return out


def _partial_spelling(ds: BuildDatasource, column: BuildColumnAssignment) -> str | None:
    """The address a column-level ``~`` was authored on, the only mark that
    licenses extension. A merge respells the column onto its target while the
    ``~`` stays recorded under the authored address.

    A table-level partial stamp (``partial datasource ... complete where``) is
    a row-subset contract the union machinery completes across siblings, and
    still relates its keys; treating it as an extension license breaks that
    assembly.
    """
    if Modifier.PARTIAL not in column.modifiers:
        return None
    for address in (column.concept.address, column.origin_concept_address):
        if address in ds.column_level_partial_addresses:
            return address
    return None


def _structural_partial(ds: BuildDatasource, column: BuildColumnAssignment) -> bool:
    return _partial_spelling(ds, column) is not None


def _proven_bound(
    proven: set[str],
    bound: set[str],
    environment: BuildEnvironment,
    authored: _AuthoredKeyspace,
) -> set[str]:
    """The bound spellings the WHERE's non-null proofs stand for.

    A derived concept is NULL wherever one of its entity keys is absent, so a
    null-rejection over it (``status = 'delivered'``, ``coalesce(amount, 0) is
    not null``) rejects the rows its keys are absent on, exactly as a proof
    over the keys' own bindings would; the keyspace names those keys
    (``keys_by_address``, what the derivation READS).
    """
    out = proven & bound
    for address in proven - bound:
        for key in authored.keyspace.keys_by_address.get(address, ()):
            concept = environment.concepts.get(key)
            out |= (_spellings(concept) if concept is not None else {key}) & bound
    return out


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


def _pair_siblings(
    key_spellings: set[str], ds: BuildDatasource, datasources: Sequence[BuildDatasource]
) -> tuple[list[BuildDatasource], list[BuildDatasource]]:
    """Sibling row-sources carrying this key inside a LARGER grain, split into
    (anchors, siblings themselves ``~`` on the key).

    An anchor supplies key combinations beyond ``ds``'s subset, so a pin that
    kills dimension extensions does not by itself shrink the population to
    ``ds``'s own rows. The heal holds only when the WHERE kills the anchor's
    rows too: they carry values only for what the anchor binds or can look up
    and are NULL elsewhere, so a proven-non-null concept outside that supply
    kills them exactly as it kills a dimension extension. A statement reading
    the anchor beside the heal is fine: the anchor is complete, so every row
    of ``ds`` has its anchor row and the merge with it may be INNER.

    A ``~`` sibling holds no full set of anything: two partial bindings have
    no defined relationship, so it never anchors (a pair-grain rollup beside
    its fact). Its rows are still rows, though: see ``_read_partials``.
    """
    anchors: list[BuildDatasource] = []
    partials: list[BuildDatasource] = []
    for other in datasources:
        if other.identifier == ds.identifier or _partition_disjoint(ds, other):
            continue
        grain = set(other.grain.components)
        if not (grain & key_spellings and grain - key_spellings):
            continue
        if any(
            _structural_partial(other, c) and c.concept.address in key_spellings
            for c in other.columns
        ):
            partials.append(other)
        else:
            anchors.append(other)
    return anchors, partials


def _lookup_supply(
    anchor: BuildDatasource, datasources: Sequence[BuildDatasource]
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


def _any_supplies_killers(
    siblings: list[BuildDatasource],
    killers: set[str],
    datasources: Sequence[BuildDatasource],
) -> bool:
    """Whether some sibling's rows carry a value for every killer, so the WHERE
    keeps them."""
    return any(killers <= _lookup_supply(s, datasources) for s in siblings)


def _read_partials(
    ds: BuildDatasource,
    partials: list[BuildDatasource],
    referenced_bound: set[str],
    datasources: Sequence[BuildDatasource],
    environment: BuildEnvironment,
) -> list[BuildDatasource]:
    """The ``~`` siblings the statement must read: each supplies a ROOT
    concept it references that ``ds`` cannot reach without it. A sibling
    serving only derived values (a rollup's ``revenue``) is a materialization
    the plan may skip, so its rows decide nothing."""
    roots = {
        addr
        for addr in referenced_bound
        if (c := environment.concepts.get(addr)) is not None
        and c.derivation == Derivation.ROOT
    }
    read: list[BuildDatasource] = []
    for p in partials:
        others = [d for d in datasources if d.identifier != p.identifier]
        if (roots - _lookup_supply(ds, others)) & _lookup_supply(p, datasources):
            read.append(p)
    return read


def _component_reach(
    ds: BuildDatasource, datasources: Sequence[BuildDatasource]
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


@dataclasses.dataclass
class _AuthoredKeyspace:
    """The statement's row universe over its bindings as authored. Healing
    runs before the reference graph exists (the graph holds the datasource
    objects, so they have to be final by then); the keyspace needs neither.
    Built on first read: most heals settle without it. Its binding facts are
    cached on ``scope``, which a plan the heal leaves unchanged reads too."""

    environment: BuildEnvironment
    scope: ScopeDatasources
    outputs: list[BuildConcept]
    conditions: list[BuildWhereClause]

    @cached_property
    def keyspace(self) -> Keyspace:
        _, attrs, _ = build_concept_graph(
            self.outputs,
            self.environment,
            self.conditions,
            datasources=self.scope.datasources,
        )
        return build_keyspace(
            attrs, self.outputs, self.environment, self.conditions, self.scope
        )


def decide_heal(
    environment: BuildEnvironment,
    scope: ScopeDatasources,
    outputs: list[BuildConcept],
    conditions: list[BuildWhereClause],
) -> dict[str, BuildDatasource]:
    """Identifier -> its binding with every ``~`` this WHERE kills the
    licensed extensions of dropped, for the bindings it changes.

    Pure over ``scope`` (the statement's bindings as authored): the
    replacements are fresh objects, and neither the environment nor the shared
    build-cache objects are written.
    """
    datasources = scope.datasources
    partial_hosts = [
        ds for ds in datasources if any(_structural_partial(ds, c) for c in ds.columns)
    ]
    if not partial_hosts:
        return {}
    proven = null_rejected(conditions)
    if not proven:
        return {}
    authored = _AuthoredKeyspace(environment, scope, outputs, conditions)
    bound = _bound_spellings(datasources)
    proven_bound = _proven_bound(proven, bound, environment, authored)
    if not proven_bound:
        return {}
    referenced_bound = (environment.statement_authored_addresses or set()) & bound
    replacements: dict[str, BuildDatasource] = {}
    for ds in partial_hosts:
        # A killer must be related to the key's own model component: a concept
        # from a disconnected subgraph attaches via a cross-join gate and is
        # non-null on extension rows too, so it proves nothing.
        reach = _component_reach(ds, datasources)
        killers = proven_bound & reach
        if not killers:
            continue
        # References outside the component (a membership set built from a
        # separately imported dimension) are sourced by their own subquery,
        # never through an anchor.
        component_refs = referenced_bound & reach
        healed: set[str] = set()
        for column in ds.columns:
            span = _partial_spelling(ds, column)
            if span is None:
                continue
            key = column.concept
            anchors, partials = _pair_siblings(_spellings(key), ds, datasources)
            # A sibling's rows the WHERE keeps hold members ``ds`` may lack: an
            # anchor's always, a `~` sibling's only when the statement reads it.
            read = anchors + _read_partials(
                ds, partials, component_refs, datasources, environment
            )
            if _any_supplies_killers(read, killers, datasources):
                continue
            if authored.keyspace.binding_is_complete(ds.identifier, span):
                healed.add(span)
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
                if _partial_spelling(ds, c) in healed
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
    return replacements


def gate_excluded_enum_values(
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


def decide_exclusion(
    datasources: Iterable[BuildDatasource], stage: BuildWhereClause
) -> set[str]:
    """Identifiers of every ``complete where`` source the statement's row
    bound rules out.

    ``stage`` is ``universal_row_bound``: the predicate every row the statement
    reads satisfies, so a source whose partition predicate contradicts it holds
    no usable row. Deciding which stages that bound may draw on belongs to the
    staging rules, not here.
    """
    return {
        ds.identifier
        for ds in datasources
        if ds.non_partial_for is not None
        and conditions_mutually_exclusive(
            stage.conditional, ds.non_partial_for.conditional
        )
    }
