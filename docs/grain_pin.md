# NULL-absorbing expressions take the select's grain

A derived concept is a function of its keys and is defined where every key is
present: `status <- case when delivery_date is not null then 'delivered' else
'in-transit' end`, keyed on the order, has no value for a customer with no order
(a `~customer_id` row). Nothing here changes that rule. What changes is WHICH keys
an expression read by a select is a function of: like a bare aggregate, a
NULL-absorbing expression takes the select's keys as inputs, so it is keyed on them
and the key-domain rule defines it on every row the select has.

## The rule

`coalesce(...)` and a `CASE ... ELSE ...` take a value where their inputs are NULL,
so where they are evaluated decides what they say on a padded row. A select
evaluates them on its own row, the way a bare aggregate groups by the select's
grain: the select's other keys become implicit inputs.

```
select customer_id, status;          -- cat (no orders): 'in-transit'
select customer_id, coalesce(amount, 0) as a;   -- cat: 0
select order_id, status;             -- unchanged: the expression's own grain
```

- **One address, one value per statement.** Grouping keys, aggregate inputs and the
  WHERE read the same pinned value, as they read the same bare aggregate:
  `where status = 'in-transit'` keeps cat, `count(status)` per customer counts cat's
  row, `sum(coalesce(amount, 0))` per customer gives cat 0. An inline expression
  pins like its named spelling.
- **A value computed from a pinned value is pinned:** `concat(name, '-', status)`
  is `'cat-in-transit'`.
- **Only keys a row can hold without the inputs pin.** A select key whose rows
  always carry the expression's inputs (the inputs' rows hold every key value, or
  every key row carries the inputs: an order carries its customer) adds nothing.
  So customer-level `case when count(order_id) by customer_id > 0 ... else ...`
  beside order rows is not pinned to the order.
- **Not pinned:** a select with no other key (`select status, count(customer_id)`
  keeps its NULL group), and a ROLLUP/CUBE select, whose outputs are grouping keys
  of the subtotal pass.
- **A rowset is a select:** a value pinned in its body is a real value of its rows.

## Persistence

The pin is part of the lineage, so its canonical name differs from the
expression's. A column persisting `status` at the order grain answers
`select order_id, status` and every select whose keys the order's rows cover, and
is never read for a select that pins it. Materialization invariance holds: the
oracle twins in `tests/helpers/models.py` bind the same derived concepts as
columns, and every query returns the same rows on both.

## Mechanism

- `FunctionType.GRAIN_PIN(expr, *anchors)` renders as `expr`
  (`trilogy/core/grain_pin.py`).
- `Factory._grain_pinned` applies it where a bare aggregate resolves its grain,
  against `Factory.select_anchors` (each output's entity keys, FD-reduced, a
  declared join's two keys folded into one); the select's projection and WHERE
  factories carry the anchors, a datasource's does not. An inline
  NULL-absorbing expression nests as its own concept, like an inline aggregate.
- The keyspace keys a pin on its anchors (`keyspace._entity_keys`), so it is
  defined on the extension region and the region domain feeds it, as it feeds an
  aggregate by the span.
- `NULL_ABSORBING_FUNCTIONS` is the registry: another function whose output the
  select grain changes is added there, with oracle cases.

A check that compares a pinned concept to a datasource column must compare
`canonical_address`, not `address`: the two share an address.
