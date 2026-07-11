# Handoff: q72 composite scoped join loses a key by projection

## Severity

Fatal framework bug with two manifestations:

- silent wrong-result joins; and
- runaway execution exceeding the eval scorer's 180-second timeout.

SQL generation is not the slow phase: the timed-out candidate generated an
8,283-character query in approximately 0.65 seconds. Execution ran away because
one component of the authored `(item, week)` match was missing in a large
intermediate branch.

## Required relation

Every q72 shape authors both scoped relations:

```preql
subset join cs.item.sk = inv.item.sk
subset join cs.sold_date.week_seq = inv.date.week_seq
```

The intended match is the composite pair:

```text
(catalog item SK, sold week sequence)
    =
(inventory item SK, inventory week sequence)
```

Warehouse is deliberately not part of the join.

## Runaway manifestation

The first July 10 candidate aggregated qualifying quantity by item/week, then
joined inventory. One generated count branch joined catalog sales to inventory
only by item and deferred/omitted the week equality. That created an all-weeks
sales-by-inventory fanout before later aggregation and scoring exceeded 180
seconds.

Artifact:

```text
evals/tpcds_agent/results/20260709-105517_enriched/workspace/query72.preql
```

The report recorded:

```text
status: timeout
detail: scoring timed out after 180s (likely planner loop or runaway query)
```

## Silent wrong-result manifestation

The next candidate staged filtered sales and again authored both item and week
joins. Its final generated inventory join retained only week sequence:

```sql
INNER JOIN inventory_rows
  ON filtered_sales.week_seq = inventory.week_seq
```

The item equality was absent, so qualifying sales matched inventory for unrelated
items in the same week. Counts and warehouses were inflated.

## Projection-sensitive proof from the live trajectory

During the active replay, `query72_check6.preql` selected sale line fields and
filtered `promotion.sk is null`. It reported one fixed catalog line:

```text
order_number = 42748
item.sk       = 16168
quantity      = 47
```

under 25 different alleged `cs.sold_date.week_seq` values ranging from 5120 to
5211. A catalog sale has one sold date and one sold week. The varying value was
being sourced/coalesced from inventory after the week relation was lost.

Small projection changes produced contradictory results:

- selecting the broad diagnostic fields showed 28 false matches for item 16168;
- adding `where cs.item.sk = 16168` returned zero;
- the final grouped shape generated both join predicates and matched the SQL
  reference once the unrelated inventory-year filter was removed.

Thus join preservation depends on selected fields/aggregate shape, not merely
on the authored scoped joins.

## Expected behavior

Every branch derived from a query-scoped composite relation must retain the
complete relation key set. The planner may reorder, preaggregate, or split
branches only if it proves the missing key is functionally implied. Here item
does not imply week and week does not imply item, so dropping either condition
is invalid.

If a requested projection cannot preserve the scoped relation, compilation must
fail clearly. It must not silently weaken the join.

## Likely root area

The bug appears when multiple query-scoped relations connect the same two model
families and discovery splits filtered aggregates or diagnostic projections into
sibling branches. Canonical/pseudonym substitution then reconstructs a branch
from only the keys needed by its selected outputs, losing the other relation
edge.

Investigate:

```text
trilogy/core/domain_graph.py
trilogy/core/processing/join_resolution.py
trilogy/core/processing/node_generators/common.py
trilogy/core/processing/nodes/merge_node.py
trilogy/core/optimizations/join_hoist.py
trilogy/core/optimizations/value_set_join_upgrade.py
```

The core invariant should live above individual optimizers: a scoped composite
relation must be represented as one relation object/key set, not as independent
edges that downstream sourcing can partially retain.

## Trigger matrix to pin

Use small `sales(order, item, sold_week, qty, promo)` and
`inventory(item, week, warehouse, on_hand)` fixtures with deliberately
overlapping item and week values.

Test all of these with the same authored item+week relations:

1. select raw sale and inventory fields;
2. group by item description, warehouse, week;
3. add filtered counts on promotion;
4. stage sales in a rowset, then join inventory;
5. add/remove an explicit item filter;
6. put scoped joins before versus after SELECT;
7. place the cross-model quantity predicate in WHERE versus supported
   post-join form.

For every variant assert:

```python
assert generated_sql_contains_item_equality
assert generated_sql_contains_week_equality
assert got == hand_written_composite_join_oracle
```

Also include a SQL-shape guard preventing a catalog-sales × all-inventory-weeks
intermediate, and a bounded execution-time regression on the SF1 q72 shape.

## Relationship to the INVALID_REFERENCE handoff

The same live trajectory also emitted an `INVALID_REFERENCE_BUG` when a raw
promotion dimension appeared in HAVING alongside a filtered aggregate. That is
documented separately in:

```text
handoff_q72_invalid_reference_filtered_aggregate_having.md
```

Both bugs drive the same agent thrash, but they have distinct fixes: condition
source resolution versus preservation of the full scoped composite key.
