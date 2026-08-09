"""The join matrix is the oracle-checked contract for scoped-join / rowset
semantics (presence probes, rowset-pair key-carry, coalescing axis).

Cells used to run twice — once per planner — while the legacy engine was still
in-tree; the parametrization went away with it.
"""
