"""Shared sourcing helpers behind the planner's ROOT/datasource selection.

What remains here is the datasource-selection layer (`select_node`,
`select_merge_node`, `select_helpers`) plus the utilities the v4 node
generators reuse; the per-derivation `gen_*` generators live in
`v4_node_generators`.
"""
