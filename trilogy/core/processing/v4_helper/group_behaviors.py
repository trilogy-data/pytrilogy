"""Per-derivation IO contracts for v4 group nodes.

`group_rules.py` answers "how do concepts cluster into a bucket?"; this
file answers "given a bucket, what are the rules for its input / output
sets?"

Two pure functions per derivation:

- ``native_grain(bucket, concept_graph) -> frozenset[str]``: the grain at
  which this group's output rows live. Drives the grain-compatibility
  check applied when deciding which upstream columns can ride through.
- ``can_preserve(concept_graph, native_grain, address) -> bool``: given
  a candidate column from a parent's capability, can we carry it through
  our SELECT without breaking row shape?

Concept lineage parents (i.e. ``primary_input_args``) is NOT per-derivation
here: ``concept_graph`` already encodes per-derivation edge fetchers (see
``concept_graph._UPSTREAM``), so walking ``lineage`` edges is enough.

The contract is intentionally tiny so it can be unit-tested per
derivation without standing up a full BuildEnvironment. The orchestrator
in ``group_graph._compute_concept_sets`` is the only consumer."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation

from .constants import EdgeKind
from .edges import EdgeMap, edge_kind
from .functional_dependency import concept_attr_fd_closure, concept_attr_fd_determines
from .models import ConceptAttrs, GroupBucket

# Behaviors read node state from `concept_attrs` (the typed side dict keyed by
# concept-graph node id) and walk lineage edges via `concept_graph` + its typed
# `concept_edges` side map; the graph itself carries only topology.
NativeGrainFn = Callable[
    [GroupBucket, nx.DiGraph, EdgeMap, dict[str, ConceptAttrs]], frozenset[str]
]
CanPreserveFn = Callable[
    [nx.DiGraph, EdgeMap, dict[str, ConceptAttrs], frozenset[str], str], bool
]


@dataclass(frozen=True)
class Behavior:
    """The IO contract for one derivation. See module docstring."""

    native_grain: NativeGrainFn
    can_preserve: CanPreserveFn


def _lineage_parents(
    concept_graph: nx.DiGraph, concept_edges: EdgeMap, address: str
) -> frozenset[str]:
    if address not in concept_graph.nodes:
        return frozenset()
    return frozenset(
        u
        for u, _ in concept_graph.in_edges(address)
        if edge_kind(concept_edges, u, address) == EdgeKind.LINEAGE
    )


# ----- native_grain implementations -----------------------------------


def native_grain_declared(
    bucket: GroupBucket,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
) -> frozenset[str]:
    """For groups whose row identity matches their declared
    ``grain_components`` (AGGREGATE / GROUP_TO / WINDOW / FILTER /
    SUBSELECT), the bucket's grain is the right answer."""
    return frozenset(bucket.grain_components)


def native_grain_filter_inputs(
    bucket: GroupBucket,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
) -> frozenset[str]:
    """FILTER rows live at the grain of the row expression being filtered.

    A virtual-filter concept may be assigned the grain of a downstream
    aggregate that consumes it (`count(x ? predicate) by rank`), but the filter
    CTE itself is still a row-preserving/subsetting stream over `x`. Use the
    lineage input grains so join keys from that row stream can ride through.
    """
    inherited: set[str] = set()
    for primary in bucket.primary_members:
        for parent in _lineage_parents(concept_graph, concept_edges, primary):
            inherited.update(concept_attrs[parent].grain_components)
            inherited.update(concept_attrs[parent].keys)
    if inherited:
        return frozenset(inherited)
    return frozenset(bucket.grain_components)


def native_grain_basic_inherited(
    bucket: GroupBucket,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
) -> frozenset[str]:
    """BASIC's effective grain is the union of its primaries' lineage
    parents' grains.

    ``concept.grain`` for a BASIC walks back through its lineage to the
    source row identity, but when the lineage sits on top of aggregate or
    window outputs those outputs have already collapsed the rows to a
    narrower grain, and that is the grain the BASIC's SQL row stream lives
    at. A scalar over two aggregates keyed by the same column runs per key,
    and that key must stay grain-compatible so it can ride through as a
    merge join key.

    Fall back to the declared grain when no lineage parents are present
    (e.g. a constant-folded BASIC)."""
    inherited: set[str] = set()
    for primary in bucket.primary_members:
        for parent in _lineage_parents(concept_graph, concept_edges, primary):
            inherited.update(concept_attrs[parent].grain_components)
    if inherited:
        return frozenset(inherited)
    return frozenset(bucket.grain_components)


# ----- can_preserve implementations -----------------------------------


def can_preserve_grain_subset(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    native_grain: frozenset[str],
    address: str,
) -> bool:
    """A column rides through iff it is functionally determined by
    ``native_grain``: the address is itself a grain key, OR its declared
    grain is a subset of ``native_grain``, OR it has no grain (constant /
    scalar).

    Subset is a syntactic approximation of functional dependency (no FK
    closure walk), so it is coarser than necessary, but safe: a blocked
    column just doesn't ride this group's CTE, and if it is reachable
    through a different parent path it still lands where it is needed."""
    return concept_attr_fd_determines(concept_attrs, native_grain, address)


def _attrs_for_address(
    concept_attrs: dict[str, ConceptAttrs], address: str
) -> ConceptAttrs | None:
    return next(
        (attrs for attrs in concept_attrs.values() if attrs.address == address),
        None,
    )


def _lineage_parent_addrs(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    address: str,
) -> set[str]:
    if address not in concept_graph.nodes:
        return set()
    return {
        concept_attrs[u].address
        for u, _ in concept_graph.in_edges(address)
        if edge_kind(concept_edges, u, address) == EdgeKind.LINEAGE
    }


def can_preserve_grouping(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    native_grain: frozenset[str],
    address: str,
) -> bool:
    """Preservation for a GROUP-BY / PARTITION-BY derivation.

    Like the subset rule, but with two adjustments for columns that aren't
    grain-subset-determined:
    - a *rename of grain keys* (every lineage parent is a grain key) rides
      through: it IS a group key under another name, and the SELECT renders it
      from the (grouped) key;
    - a bare empty-grain column rides through only if it's a true CONSTANT. A
      row-varying empty-grain value (a CASE that isn't a key rename) would land
      in the SELECT with no GROUP BY entry, which is invalid SQL."""
    node = _attrs_for_address(concept_attrs, address)
    if node is None:
        return False
    # Nothing is proven through an empty-grain FILTER virtual's keys; the
    # closure itself enforces that (`ConceptAttrs.keys_are_conditional_fd`), so
    # such a virtual reaches the lineage-parents rule below instead.
    closure = concept_attr_fd_closure(
        concept_attrs, native_grain, include_empty_grain=False
    )
    if address in closure:
        return True
    col_grain = node.grain_components
    parents = _lineage_parent_addrs(
        concept_graph, concept_edges, concept_attrs, address
    )
    if parents and parents <= closure:
        return True
    if not col_grain:
        return node.derivation == Derivation.CONSTANT
    return False


# ----- registry --------------------------------------------------------

# Default behavior: declared grain + subset preservation. Used for any
# derivation not enumerated below (ROOT is the scan itself and never asks
# about preservation; RECURSIVE/UNION/ROWSET edge cases): safe because
# subset preservation is the conservative answer.
_DEFAULT_BEHAVIOR = Behavior(
    native_grain=native_grain_declared,
    can_preserve=can_preserve_grain_subset,
)

GROUP_BEHAVIORS: dict[Derivation, Behavior] = {
    Derivation.BASIC: Behavior(
        native_grain=native_grain_basic_inherited,
        can_preserve=can_preserve_grain_subset,
    ),
    Derivation.AGGREGATE: Behavior(
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grouping,
    ),
    Derivation.GROUP_TO: Behavior(
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grouping,
    ),
    Derivation.WINDOW: Behavior(
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grouping,
    ),
    Derivation.FILTER: Behavior(
        native_grain=native_grain_filter_inputs,
        can_preserve=can_preserve_grain_subset,
    ),
    Derivation.SUBSELECT: Behavior(
        native_grain=native_grain_declared,
        can_preserve=can_preserve_grain_subset,
    ),
    # UNNEST expands each source row into one row per element; every source-row
    # column rides through unchanged. Preservation is judged against the SOURCE
    # row grain: the declared grain is the element value, which FD-determines
    # nothing, so the default behavior would strip the source keys and leave a
    # sibling merge with no axis.
    Derivation.UNNEST: Behavior(
        native_grain=native_grain_filter_inputs,
        can_preserve=can_preserve_grain_subset,
    ),
}


def behavior_for(derivation: Derivation | None) -> Behavior:
    """Return the behavior for ``derivation``, falling back to the
    conservative default for derivations we haven't enumerated."""
    if derivation is None:
        return _DEFAULT_BEHAVIOR
    return GROUP_BEHAVIORS.get(derivation, _DEFAULT_BEHAVIOR)
