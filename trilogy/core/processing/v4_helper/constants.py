from typing import Literal

from trilogy.core.enums import Derivation

EdgeKind = Literal["lineage", "constraint", "existence", "merge"]

EDGE_KIND_LINEAGE: EdgeKind = "lineage"
EDGE_KIND_CONSTRAINT: EdgeKind = "constraint"
EDGE_KIND_EXISTENCE: EdgeKind = "existence"
EDGE_KIND_MERGE: EdgeKind = "merge"

DEPENDENCY_EDGE_KINDS: frozenset[EdgeKind] = frozenset(
    (EDGE_KIND_LINEAGE, EDGE_KIND_CONSTRAINT, EDGE_KIND_EXISTENCE)
)

# Derivations that change row shape — a filter cannot be safely pushed below
# one of these; it must be applied above the barrier instead.
ROW_SHAPE_BARRIER_DERIVATIONS: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.GROUP_TO,
    Derivation.UNION,
    Derivation.RECURSIVE,
    Derivation.ROWSET,
}

# Derivations whose row shape is defined by a grain/by/partition list —
# grain components can be pulled into the group for free since they are
# part of the GROUP BY (or PARTITION BY) clause already.
GROUPING_DERIVATIONS: set[str] = {
    Derivation.AGGREGATE.value,
    Derivation.WINDOW.value,
    Derivation.GROUP_TO.value,
}

LABEL_DEPTH = "depth_label"

FINAL_NODE_ID = "__final__"
