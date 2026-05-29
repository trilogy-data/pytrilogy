from trilogy.core.enums import Derivation

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
