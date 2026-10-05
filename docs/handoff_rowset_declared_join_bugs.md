# Handoff: declared rowset joins, what is still open

Cases 1 and 2 of the original handoff, plus a filtered-rowset row drop found
while fixing them, are fixed and pinned in
`tests/engine/test_rowset_declared_join_pairing.py` and
`tests/join_matrix/test_subset_join_rowset_onto_root.py`. The `union join`
aggregate widening is fixed too: the coalesced axis now pairs the aggregate's
input stream instead of widening its grouping grain
(`_aggregate_coalesced_axis`, pinned in
`tests/engine/test_duckdb_rowset.py::test_composite_union_join_rowset_*`).
One item remains.

## The `subset join` hint for an FK output (diagnostic, not fixed)

On `preql-demo/docs/examples` the setup merges `order.customer_id`, but the
rowset reads `orders.customer_id`, a separate import with no merge
(`orders.customer_id` only merges into `orders.customer.id`, its own file's
alias). `customers.*` sits in a component of its own: the two are related only
by reading the same physical tables, so there is no merge for
`rowset_relation_hints` to follow. A hint there needs a "same datasource
address" heuristic, which is a product call.
