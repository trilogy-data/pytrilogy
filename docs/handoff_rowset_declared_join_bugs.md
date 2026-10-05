# Handoff: declared rowset joins, what is still open

Cases 1 and 2 of the original handoff, plus a filtered-rowset row drop found
while fixing them, are fixed and pinned in
`tests/engine/test_rowset_declared_join_pairing.py` and
`tests/join_matrix/test_subset_join_rowset_onto_root.py`. Two items remain.

## 1. An aggregate over a `union join` rowset relation splits per axis row (pre-existing)

`_aggregate_axis_members` (`v4_helper/concept_graph.py`) widens an aggregate's
grouping grain by the relation axis its inputs ride. That is now limited to a
relation the `by` names, or a coalescing (`union join`) one. Under `subset
join` it split `sum(rs.amt)` by `customer_id` into one row per order, which was
case 1.

The `union join` widening is still wrong rows, and
`tests/engine/test_duckdb_rowset.py::test_composite_union_join_rowset_*` pins
them: `select state, stddev(quantity), count(r_filtered.return_quantity)
union join ticket = r_filtered.r_ticket union join item = r_filtered.ritem`
returns two `CA` rows with a NULL stddev, where the answer is one `CA` row,
`stddev(5, 7) = 1.414`. Dropping the union widening too gives the right rows
for one and two keys. For three keys (`quantity = r_filtered.return_quantity`,
nothing matches), it then loses the return-only, NULL-`state` group. The FINAL
merge of the count aggregate with the stddev aggregate types INNER, because the
count side's `state` is not known nullable after the FULL join that fed it.
Fixing it needs that nullability carried through the aggregate, then the union
widening dropped and the pinned rows re-ruled.

## 2. The `subset join` hint for an FK output (diagnostic, not fixed)

On `preql-demo/docs/examples` the setup merges `order.customer_id`, but the
rowset reads `orders.customer_id`, a separate import with no merge
(`orders.customer_id` only merges into `orders.customer.id`, its own file's
alias). `customers.*` sits in a component of its own: the two are related only
by reading the same physical tables, so there is no merge for
`rowset_relation_hints` to follow. A hint there needs a "same datasource
address" heuristic, which is a product call.
