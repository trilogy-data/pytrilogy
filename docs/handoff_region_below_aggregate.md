# Handoff: a region's rows below an arbitrary aggregate

Branch `extension-row-null-semantics` (PR #702), as of `26f8cbf51`.

## The bug

When a statement demands an extension region (customers no `~` order
references) and the region can only be read *below* an aggregate that is not
grouped by the region's key, the planner falls back to the **padded plan**: one
stream of `customers LEFT JOIN orders`, with every row-level derivation over the
fact evaluated on top of it. A derivation over order columns is then computed on
the orderless customer's padded row instead of being NULL there.

Models: `_DERIVED` / `_MATERIALIZED` in `tests/engine/test_derived_key_domain.py`
(customers 1 ann, 2 bob, 3 cat; orders 100 cust1 delivered, 101 cust1
undelivered, 102 cust2 delivered; `orders.customer_id` is `~`). `status` is a
CASE over order columns on `_DERIVED` and a stored column on `_MATERIALIZED`, so
the two must agree; on `_MATERIALIZED` the padded row's `status` is NULL.

Shapes still wrong on `_DERIVED` (hand-check exact rows before asserting):

- ROLLUP: `select customer_id, count(status) as n by rollup (customer_id)` —
  cat's row counts the padded `status`, and the grand total is inflated.
- A statement-wide gate the region's rows feed:
  `select customer_id, status where count(customer_id) by * > 2` returns
  `(3, 'in-transit')`; want `(3, None)`. Same for `label`, `flag`.

## Why it is not fixed yet

The keyspace already says which rows are padding: `Keyspace` regions
(`trilogy/core/models/keyspace.py`: `Region.present` / `spans`, `region_of`,
`defined_on`, `carried_on`), and the region domain built per demanded region
(`v4_helper/region_domains.py`, `region_reads.py`), whose rows *are* the
region's members. The fix must read presence off that, not off key values.

What is missing is a way to **plan** with it. Today a region domain's rows join
back only:

- at FINAL, or
- under an aggregate grouped by the region's own key (`customer_id`).

Nothing joins the domain to the solid stream *below an arbitrary aggregate*. A
ROLLUP over `customer_id` and an `avg(bal) by *` over all customers both need
that: the aggregate's input must be `solid rows UNION/JOIN region rows`, with the
derivation evaluated on the solid rows before the region's members are added.

Earlier attempts:

- `26f8cbf51` handled the case where the aggregate is *absent* on the region and
  the region's rows do not feed it: the region gets its own domain, `status` is
  computed on the orders' own rows, and the join makes it NULL. Read it first;
  it is the pattern to extend.
- Dropping the rollup exclusion fixed one rollup shape but broke 4 cases in
  `tests/engine/test_duckdb_rollup_region_domain.py`; reverted.
- The `by *` exclusion (`396f0805e`) exists because TPC-H q22 (`avg(bal) by *`
  beside a region) returned no rows without it.

## Do not

- Guard on a key's value (`CASE WHEN key IS NULL THEN NULL ...`). Backed out in
  `bdbed7102` and rejected again on 2026-10-02: `?` keys are NULL on real rows,
  ROLLUP grand-total rows have NULL keys (it NULLed real TPC-DS q05 totals), and
  a property is never a witness (q70 window fan-out hang).
- Invent a new presence marker; the keyspace has what is needed.

## Must stay correct

- `test_nullable_key_beside_a_region_domain` (`?` store key beside a domain).
- `test_rollup_subtotal_row_keeps_its_value`,
  `tests/engine/test_duckdb_rollup_region_domain.py`.
- TPC-H q22 rows; no `zquery<N>.log` may gain a CTE.

## Related open bugs (separate, also wrong)

1. Null-safe FINAL join matches a padded NULL to a real NULL group:
   `pstatus <- case when amount > 15 then 'big' end` (NULL on real orders);
   `select customer_id, pstatus where coalesce(sum(amount) by pstatus, 0) = 0`
   drops cat, whose absent NULL pairs with the real NULL group. Wrong at baseline.
2. `select customer_id, status where customer_id >= avg(customer_id) by * and
   status is null` returns `[]`; want `[(3, None)]`. The WHERE's own scan
   computes `status` from orders and INNER-joins customers.
3. Padding nobody claims when the region key is not selected:
   `select city, status where count(customer_id) by * > 2` drops
   `('y', None)`; no group exposes `customer_id`, so extent election picks no
   owner and every join is INNER (`extent_ownership.py`; hidden-span carrying
   only works for regions with their own domain group).
4. ROLLUP fan-out: `select customer_id, count(order_id) as n, count(return_id)
   as r by rollup (customer_id)` (with a `returns` fact) gives ann r=2, want 1.
5. `select status, count(customer_id) as c, sum(amount) as s where name =
   'cat'` returns `[]`; want `[(None, 1, None)]` (the `activity = 'dormant'`
   spelling works).
6. Two bare `if not outputs: continue` skips remain in the strategy builder's
   build loop (`340fc47c7` replaced the third with an explicit rule); the
   islands case drops aggregate readers through them silently.

## Working notes

- Run tests one directory at a time; the machine is memory-tight. When a local
  run stalls or is reaped, push and watch CI on PR #702.
- Verify by rows on both models, then a corpus SQL A/B
  (`local_scripts/sql_ab/README.md`).
