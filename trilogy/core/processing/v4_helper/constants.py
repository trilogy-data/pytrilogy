from enum import Enum

from trilogy.core.enums import Derivation


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
    # Authored join-axis equality between two DIFFERENT lineages (a
    # statement-scoped relation member and its canonical). Rows JOIN across it;
    # it is never a computational dependency, so lineage reach must not follow
    # it: the sides recombine at a merge, not inside one scan.
    RELATION = "relation"


class EdgePhase(Enum):
    """Whether an edge sits before or after the conditions are applied."""

    PRE_CONDITION = "pre_condition"
    POST_CONDITION = "post_condition"


# Edge kinds that express a build-ordering dependency (the source must be built
# before the consumer). MERGE is excluded: it only feeds the FINAL sink.
DEPENDENCY_EDGE_KINDS: frozenset[EdgeKind] = frozenset(
    (EdgeKind.LINEAGE, EdgeKind.CONSTRAINT, EdgeKind.EXISTENCE)
)

# Derivations that change row shape. A filter cannot be pushed below one of
# these; it must be applied above the barrier instead.
ROW_SHAPE_BARRIER_DERIVATIONS: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.GROUP_TO,
    Derivation.UNION,
    Derivation.RECURSIVE,
    Derivation.ROWSET,
}

# Derivations whose row shape is defined by a grain/by/partition list. Grain
# components can be pulled into the group for free since they are already part
# of the GROUP BY (or PARTITION BY) clause.
GROUPING_DERIVATIONS: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.WINDOW,
    Derivation.GROUP_TO,
}

# Derivations evaluated against their input row stream as it stands: per row,
# or (WINDOW) one row ranked against the others. A row padded for a ``~``
# extension is an input like any other, so it gets a value, or takes a rank, it
# has no entity to own. An aggregate is not one: it is evaluated OVER the
# extended rows (`count(order_id)` is 0 for a customer with no order).
ROW_STREAM_DERIVATIONS: set[Derivation] = {
    Derivation.BASIC,
    Derivation.FILTER,
    Derivation.WINDOW,
}

FINAL_NODE_ID = "__final__"
