"""Per-derivation IO contracts for v4 group nodes.

`group_rules.py` answers "how do concepts cluster into a bucket?" — this
file answers "given a bucket, what are the rules for its input / output /
hidden sets?"

Two pure functions per derivation:

- ``native_grain(bucket, concept_graph) -> frozenset[str]`` — the grain at
  which this group's output rows live. Drives the grain-compatibility
  check applied when deciding which upstream columns can ride through.
- ``can_preserve(concept_graph, native_grain, address) -> bool`` — given
  a candidate column from a parent's capability, can we carry it through
  our SELECT without breaking row shape?

Concept lineage parents (i.e. ``primary_input_args``) is NOT per-derivation
here: ``concept_graph`` already encodes per-derivation edge fetchers (see
``concept_graph._UPSTREAM``), so walking ``lineage`` edges is enough.

The contract is intentionally tiny so it can be unit-tested per
derivation without standing up a full BuildEnvironment. The orchestrator
in ``group_graph._compute_concept_sets`` is the only consumer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import networkx as nx

from trilogy.core.enums import Derivation

from .models import GroupBucket

NativeGrainFn = Callable[[GroupBucket, nx.DiGraph], frozenset[str]]
CanPreserveFn = Callable[[nx.DiGraph, frozenset[str], str], bool]


@dataclass(frozen=True)
class Behavior:
    """The IO contract for one derivation. See module docstring."""

    derivation: str
    native_grain: NativeGrainFn
    can_preserve: CanPreserveFn


def _lineage_parents(concept_graph: nx.DiGraph, address: str) -> frozenset[str]:
    if address not in concept_graph.nodes:
        return frozenset()
    return frozenset(
        u
        for u, _, d in concept_graph.in_edges(address, data=True)
        if d.get("kind") == "lineage"
    )


# ----- native_grain implementations -----------------------------------


def native_grain_root(bucket: GroupBucket, concept_graph: nx.DiGraph) -> frozenset[str]:
    """ROOT is the scan. There's no row-shape change to defend against, so
    we return an empty grain and let `can_preserve_root` short-circuit."""
    return frozenset()


def native_grain_declared(
    bucket: GroupBucket, concept_graph: nx.DiGraph
) -> frozenset[str]:
    """For groups whose row identity matches their declared
    ``grain_components`` (AGGREGATE / GROUP_TO / WINDOW / FILTER /
    SUBSELECT), the bucket's grain is the right answer."""
    return frozenset(bucket.grain_components)


def native_grain_basic_inherited(
    bucket: GroupBucket, concept_graph: nx.DiGraph
) -> frozenset[str]:
    """BASIC's effective grain is the union of its primaries' lineage
    parents' grains.

    Why not ``bucket.grain_components``? The trilogy compiler reports
    ``concept.grain`` for a BASIC by walking back through its lineage to
    the underlying source row identity — but when the BASIC's lineage
    sits on top of aggregate or window outputs, those outputs have
    already collapsed the source rows to a narrower grain. The BASIC's
    actual SQL row stream lives at the narrower grain, not the source
    grain.

    Example (TPC-DS q02): ``round(sum(price) by week_seq / lead(...) over
    (... by week_seq), 2)`` has ``concept.grain`` reporting the source
    row grain (item.id, order_id, sales_channel), but the round() runs
    per-week (the aggregates' grain). Treating week_seq as
    grain-incompatible would block the BASIC from carrying it forward
    as a merge join key.

    Fall back to the declared grain when no lineage parents are present
    (e.g. a constant-folded BASIC)."""
    inherited: set[str] = set()
    for primary in bucket.primary_members:
        for parent in _lineage_parents(concept_graph, primary):
            inherited.update(
                concept_graph.nodes[parent].get("grain_components", frozenset())
            )
    if inherited:
        return frozenset(inherited)
    return frozenset(bucket.grain_components)


# ----- can_preserve implementations -----------------------------------


def can_preserve_root(
    concept_graph: nx.DiGraph, native_grain: frozenset[str], address: str
) -> bool:
    """ROOT exposes only its primaries (the scan). There's no upstream
    to preserve from. Returning False here is a safety net — the
    orchestrator never asks ROOT about preservation because ROOT has
    no predecessors."""
    return False


def can_preserve_grain_subset(
    concept_graph: nx.DiGraph, native_grain: frozenset[str], address: str
) -> bool:
    """A column rides through iff it is functionally determined by
    ``native_grain``: the address is itself a grain key, OR its declared
    grain is a subset of ``native_grain``, OR it has no grain (constant /
    scalar).

    Subset is a syntactic approximation of functional dependency. We
    don't walk FK closures, so this is coarser than necessary — a column
    with declared grain {date.id} can't ride through a group at grain
    {item.id, order_id, sales_channel} even if every sales row has a
    unique date.id. The safety property holds: a blocked column just
    doesn't ride this group's CTE; if it's reachable through a different
    parent path, it'll still land where it's needed."""
    if address in native_grain:
        return True
    if address not in concept_graph.nodes:
        return False
    col_grain = concept_graph.nodes[address].get("grain_components", frozenset())
    if not col_grain:
        return True
    return col_grain <= native_grain


def _lineage_parent_addrs(concept_graph: nx.DiGraph, address: str) -> set[str]:
    if address not in concept_graph.nodes:
        return set()
    return {
        concept_graph.nodes[u].get("address", u)
        for u, _, d in concept_graph.in_edges(address, data=True)
        if d.get("kind") == "lineage"
    }


def can_preserve_grouping(
    concept_graph: nx.DiGraph, native_grain: frozenset[str], address: str
) -> bool:
    """Preservation for a GROUP-BY / PARTITION-BY derivation.

    Like the subset rule, but with two adjustments for columns that aren't
    grain-subset-determined:
    - a *rename of grain keys* (every lineage parent is a grain key) rides
      through — it IS a group key under another name, and the SELECT renders it
      from the (grouped) key (q05 `s_channel`←`channel_label`,
      `s_id`←`sales_id_label` over a ROLLUP);
    - a bare empty-grain column rides through only if it's a true CONSTANT — a
      row-varying empty-grain value (a CASE that isn't a key rename) would land
      in the SELECT with no GROUP BY entry, which is invalid SQL."""
    if address in native_grain:
        return True
    if address not in concept_graph.nodes:
        return False
    node = concept_graph.nodes[address]
    col_grain = node.get("grain_components", frozenset())
    if col_grain and col_grain <= native_grain:
        return True
    parents = _lineage_parent_addrs(concept_graph, address)
    if parents and parents <= native_grain:
        return True
    if not col_grain:
        return node.get("derivation") == Derivation.CONSTANT.value
    return False


# ----- registry --------------------------------------------------------

# Default behavior: declared grain + subset preservation. Used for any
# derivation we haven't enumerated below (RECURSIVE/UNION/ROWSET edge
# cases) — safe because subset preservation is the conservative answer.
_DEFAULT_BEHAVIOR = Behavior(
    derivation="",
    native_grain=native_grain_declared,
    can_preserve=can_preserve_grain_subset,
)

GROUP_BEHAVIORS: dict[str, Behavior] = {
    Derivation.ROOT.value: Behavior(
        derivation=Derivation.ROOT.value,
        native_grain=native_grain_root,
        can_preserve=can_preserve_root,
    ),
    Derivation.BASIC.value: Behavior(
        derivation=Derivation.BASIC.value,
        native_grain=native_grain_basic_inherited,
        can_preserve=can_preserve_grain_subset,
    ),
    Derivation.AGGREGATE.value: Behavior(
        derivation=Derivation.AGGREGATE.value,
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grouping,
    ),
    Derivation.GROUP_TO.value: Behavior(
        derivation=Derivation.GROUP_TO.value,
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grouping,
    ),
    Derivation.WINDOW.value: Behavior(
        derivation=Derivation.WINDOW.value,
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grouping,
    ),
    Derivation.FILTER.value: Behavior(
        derivation=Derivation.FILTER.value,
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grain_subset,
    ),
    Derivation.SUBSELECT.value: Behavior(
        derivation=Derivation.SUBSELECT.value,
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grain_subset,
    ),
}


def behavior_for(derivation: str) -> Behavior:
    """Return the behavior for ``derivation``, falling back to the
    conservative default for derivations we haven't enumerated."""
    return GROUP_BEHAVIORS.get(derivation, _DEFAULT_BEHAVIOR)
