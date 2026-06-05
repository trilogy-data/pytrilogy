from enum import Enum
from typing import NewType

from trilogy.core.enums import Derivation

# --- identifier aliases -------------------------------------------------
# Distinct static identities for the three string namespaces the planner
# threads around, so mypy flags mixing a concept address with a concept-graph
# node id (the bracketed ``[label]address`` form) or a group id.
ConceptAddress = NewType("ConceptAddress", str)
ConceptNodeId = NewType("ConceptNodeId", str)
GroupId = NewType("GroupId", str)


class DepthLabel(Enum):
    """Placement role of a concept (or group) in the v4 plan."""

    STAR = "d*"  # ordinary row-level concept
    D0 = "d0"  # row-shape-barrier output (aggregate / window / unnest / ...)
    D1 = "d1"  # reached via the WHERE (condition-phase) recursion
    ROOT = "root"  # datasource scan group
    ROOT_D1 = "root_d1"  # pristine scan dedicated to feeding d1 calculations
    FINAL = "final"  # the FINAL sink


class EdgeKind(Enum):
    """How a directed edge relates two nodes in the concept / group graph."""

    LINEAGE = "lineage"  # computational dependency; rows flow along it
    CONSTRAINT = "constraint"  # d1→d0 must-be-above ordering (implied JOIN)
    EXISTENCE = "existence"  # side-channel subselect source (IN/EXISTS RHS)
    MERGE = "merge"  # group → FINAL sink


class EdgePhase(Enum):
    """Whether an edge sits before or after the conditions are applied."""

    PRE_CONDITION = "pre_condition"
    POST_CONDITION = "post_condition"


# Edge kinds that express a build-ordering dependency (the source must be built
# before the consumer). MERGE is excluded — it only feeds the FINAL sink.
DEPENDENCY_EDGE_KINDS: frozenset[EdgeKind] = frozenset(
    (EdgeKind.LINEAGE, EdgeKind.CONSTRAINT, EdgeKind.EXISTENCE)
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
GROUPING_DERIVATIONS: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.WINDOW,
    Derivation.GROUP_TO,
}

FINAL_NODE_ID = "__final__"
