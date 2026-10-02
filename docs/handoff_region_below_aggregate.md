# Handoff: a region's rows below an arbitrary aggregate

Branch `extension-row-null-semantics` (PR #702). The two shapes this handoff was
opened for are fixed; what follows is the rule that fixed them, what is still
open, and a design note on ROLLUP.

Models: `_DERIVED` / `_MATERIALIZED` in `tests/engine/test_derived_key_domain.py`
(customers 1 ann, 2 bob, 3 cat; orders 100 cust1 delivered, 101 cust1
undelivered, 102 cust2 delivered; `orders.customer_id` is `~`). `status` is a
CASE over order columns on `_DERIVED` and a stored column on `_MATERIALIZED`, so
the two must agree. The twin is blind wherever both models pad alike: hand-check
rows before asserting.

## What was wrong

A demanded extension region fell back to the padded plan (`DomainKind.PADDED`)
in two cases, and a derivation over the fact was then evaluated on the
orderless customer's padded row:

- a ROLLUP over a key the region carries (`note="rollup key"`);
- a statement-wide gate the region's rows feed (`count(customer_id) by * > 2`).

## The rule now

The padded plan is right exactly while nothing takes a value on a padded row,
and it is the smaller plan, so both cases keep it until something does
(`_named_value_on_padding` / `_inline_value_on_padding` in `region_domains.py`).
When something does, the region gets its own domain and:

- **ROLLUP.** The domain feeds the pass below it, on the pass's input
  (`evaluated_over_region(one_pass=True)`); its named BASIC arguments are
  projected on the solid side first (`_project_basic_aggregate_inputs`). Nothing
  above the pass is a region reader: a group reading the pass is not solid
  (`rows_above_a_rollup`), so `coalesce(sum(qty), 0)` over the pass no longer
  keeps the pass off the domain, and regraft then hands a renamed key back to
  the pass instead of the domain. A pass grouped by a key *absent* on the region
  takes the region's rows when one member counts them (`count(customer_id) by
  rollup (status)`); the `grouping()` flag is not an aggregate over rows. A
  per-member atom (`count(order_id) by customer_id < 2`) is hosted on the pass's
  input, since FINAL would test the subtotal rows.
- **`by *` gate.** `keyless` no longer excludes a gate the region feeds. The
  gate joins the *united* rows of an aggregate's input, not the solid stream
  (it was NULL on the padded row there), and a WHERE aggregate reading only
  what the domain holds reads the domain alone (`_counts_the_domain`): its
  shared condition scan, joined to a fact, holds only the customers an order
  references.
- **Extent election** (padded plans). A scan the WHERE reads for itself loses a
  tie to the statement's row stream, and a group holding the span as a member
  stands for election even when it does not output it. Both were INNER-joining
  the fact under the padded plan (`select name, status where count(customer_id)
  by * > 2` on the materialized model).

TPC-H q22 keeps its plan (nothing there takes a value on padding); the corpus
A/B moved 0 of 200 statements.

## Still open

1. **Inline argument under a ROLLUP.** `sum(coalesce(amount, 0)) by rollup
   (customer_id)` and `array_agg(amount)` beside `count(status)` keep the padded
   plan (cat's total is 0, want NULL; `count(status)` counts her). An inline
   argument has no node to compute on the solid rows first.
2. Null-safe FINAL join matches a padded NULL to a real NULL group:
   `pstatus <- case when amount > 15 then 'big' end`;
   `select customer_id, pstatus where coalesce(sum(amount) by pstatus, 0) = 0`
   drops cat.
3. ROLLUP fan-out: `select customer_id, count(order_id) as n, count(return_id)
   as r by rollup (customer_id)` (with a `returns` fact) gives ann r=2, want 1.
4. `select status, count(customer_id) as c, sum(amount) as s where name =
   'cat'` returns `[]`; want `[(None, 1, None)]`.
5. A `by *` aggregate as an OUTPUT beside a region: `select customer_id, status,
   count(customer_id) by * as total` on `_DERIVED` pairs every customer with
   every status. No region domain is decided: the root demand is split into two
   components (`customer_id` | `delivery_date`) before the region step, and
   FINAL cross-joins them. Wrong at baseline.
6. A region demanded only through a WHERE aggregate counting it: `select status,
   count(order_id) as o where count(customer_id) by customer_id > 0` returns
   `(None, 0)` on `_MATERIALIZED` and not on `_DERIVED`. Decide which is right.
7. `count(<key>)` under a ROLLUP sums the finest-grain rows on subtotal rows
   (`count(customer_id) by rollup (name, status)` gives ann's subtotal 2, the
   grand total 4): the pass is one GROUP BY ROLLUP over rows normalized at the
   finest grain.
8. Two bare `if not outputs: continue` skips remain in the strategy builder's
   build loop.

Fixed here and no longer open: the `avg(customer_id) by *` + `status is null`
shape, and padding nobody claims when the region key is not selected.

## Design note: ROLLUP as a function over a relation

Every fix above is a carve-out for one fact: the same address (`customer_id`)
means a row key below the pass and a nullable subtotal label above it, and the
keyspace's questions (defined on, carried on, who reads the domain) only make
sense below. About 80 sites already special-case this (`nulls_grouping_keys`,
`rollup_padded_keys`, `nonstandard_grouping_lineage`).

Spelling the pass's input as an explicit rowset already gives the right rows,
at baseline, with no ROLLUP-specific region logic:

```
with b as select customer_id, order_id, status;
select b.customer_id, count(b.status) as n by rollup (b.customer_id);
```

The body is a row-level statement, so the region's rows unite with the solid
ones at the body's FINAL, which is the one place the planner already joins a
domain back; above the boundary the pass reads handles keyed on the rowset and
no region exists. That spelling is right for every shape in this handoff, and
also for open items 1, 4 and 5; it renders the same SQL the fix above produces
for `count(status) by rollup (customer_id)`.

So modelling ROLLUP (and a `by *` aggregate) as a function over a body relation
with outputs of its own would retire these carve-outs rather than add to them.
It does not need new syntax: `by rollup` can desugar to the body at plan time.
What it does not buy:

- plan size: the body takes an OWN domain where the padded plan is smaller
  (the partial-date fixture goes 4 -> 6 CTEs), so the "padded while nothing
  takes a value" economy has to move into the boundary decision
  (`DomainKind.BOUNDARY` / `_needs_solid_rows` is that rule for rowsets);
- open items 3 and 7: two facts cannot share one body without fanning out, and
  a key counted on a subtotal row needs COUNT(DISTINCT) or a pass per input
  grain joined on the pass's row identity (keys plus grouping flags).

## Do not

- Guard on a key's value (`CASE WHEN key IS NULL THEN NULL ...`): `?` keys are
  NULL on real rows, ROLLUP grand-total rows have NULL keys, and a property is
  never a witness.
- Invent a new presence marker; the keyspace has what is needed.

## Working notes

- Run tests one directory at a time; the machine is memory-tight.
- Verify by rows on both models, then a corpus SQL A/B
  (`local_scripts/sql_ab/README.md`). A probe script run from a worktree must
  put cwd first on `sys.path`, or it imports the main tree's `trilogy`.
