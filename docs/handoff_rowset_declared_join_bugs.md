# Handoff: two wrong-answer bugs in declared rowset joins (pre-existing on main)

Found 2026-10-05 while documenting the PR #702 rowset islanding rule. Both
reproduce identically on `origin/main`, so neither is a regression of #702; #702
only changed the join-less spelling from a silent cross product to
`DisconnectedConceptsException`.

Model for both: `tests/helpers/models.py::CUSTOMERS_DERIVED` (customers 1 ann,
2 bob, 3 cat; orders 100/101 for ann at 10/20, 102 for bob at 30; cat has no
order). Rows via `tests/helpers/rows.sorted_rows`.

## 1. Aggregate over a rowset is dropped when the join is on the rowset's row key

```
with rs as select order_id as oid, amount as amt;
select customer_id, name, sum(rs.amt) as spend subset join rs.oid = order_id;
```

Expected (what `select customer_id, name, sum(amount)` returns):
`[(1, 'ann', 30), (2, 'bob', 30), (3, 'cat', None)]`.

Actual: `[(1, 'ann', 10), (1, 'ann', 20), (2, 'bob', 30), (3, 'cat', None)]`.
`count(rs.oid)` gives `1, 1, 1, 0` the same way.

The rendered SQL has no `sum(` at all: the CTE pairing `rs` with `orders`
projects `rs_amt` as `spend` bare, and the FINAL groups by `customer_id, name,
spend`. The aggregate's `by` (the select grain, `customer_id`) is reached from
the superset side of the declared join, and somewhere between the rowset read
and the FINAL the aggregate is treated as already at grain.

Works when the rowset projects the grouping key itself and the join is on it:

```
with rs2 as select customer_id as cid, order_id as oid, amount as amt;
select customer_id, name, sum(rs2.amt) as spend subset join rs2.cid = customer_id;
-> [(1, 'ann', 30), (2, 'bob', 30), (3, 'cat', None)]
```

## 2. Declared join beside only a PROPERTY of the superset key is a keyless join

Fixture: `preql-demo/docs/examples/models` with `examples/setup/query.preql`
(any `customer`/`customers` alias; the model-level merge is irrelevant).

```
rowset big_orders <-
where orders.total > 100
select orders.customer_id as customer_id, orders.id as id, orders.total as total;

select customers.name, count(big_orders.id) as n
subset join big_orders.customer_id = customers.id;
```

Raises `UnresolvableQueryException: Planner emitted a keyless join between
row-bearing sources ... This is a planner bug.` Projecting the key beside the
property (`select customers.id, customers.name, count(...)`) or the key alone
plans fine. The subset join's superset key is not an output, so the pairing
loses its axis when the customer side is grouped by `name` only; `name` is a
property of `customers.id`, which should carry the axis.

## 3. (Diagnostic, not correctness) the `subset join` hint does not fire for an FK output

For the join-less spelling of case 2 the `DisconnectedConceptsException` lists
the subgraphs but not the `subset join big_orders.customer_id = customers.id`
hint, because `rowset_relation_hints` looks for a rowset output whose body
content lands in the other subgraph, and `orders.customer_id` is an FK that
merges into `customer.id` rather than being `customers.id`. The hint fires for
`with rs as select order_id as oid, ...; select customer_id, sum(rs.amt)`
(`subset join rs.oid = order_id`). Worth following the merge when looking for
the landing component.

## Where to pin

`tests/engine/test_rowset_declared_join_pairing.py` already has the model and
the `SUBSET` spelling; cases 1 and 2 are two rows tests there. Oracle for case
1: the same select over `amount` directly.
