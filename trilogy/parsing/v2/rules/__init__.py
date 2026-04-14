"""Per-syntax-kind hydration rules for the v2 parser.

Each module in this package defines the hydrator functions for a family of
syntax nodes and exports a ``*_NODE_HYDRATORS`` mapping that the top-level
orchestration (``trilogy.parsing.v2.hydration``) composes into the global
dispatch table.
"""
