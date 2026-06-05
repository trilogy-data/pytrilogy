# Bug + fix plan: aggregate-`?` filter over a partial FK drops zero-match dimension entities

**Status:** FIXED (2026-06-05). The disjoint-pushdown co-sourcing guard
(`filter_node._optional_cosourced_with_content`) now requires the co-sourcing
datasource to cover the group keys *non-partially* (`full_concepts`). A partial
FK (`orders.customer.id`) no longer qualifies, so the predicate is not lifted to
a WHERE above the OUTER join — the planner falls back to the null-safe
`count(CASE WHEN cond THEN x END)` form, preserving zero-match dim entities.
Repro test `tests/optimization/test_not_like_partial_join.py` is green (xfail
removed; `=`, `!=`, `is not null` variants added). Implemented as seam #1 below
(inline CASE) via pushdown suppression rather than fact-key plumbing.

Original root-cause analysis preserved below.
```
import order as orders;
metric orders_per_customer <- count( orders.id ? orders.comment not like '%special%requests%') by orders.customer.id;
SELECT
    coalesce(orders_per_customer,0) -> c_count,
    count(orders.customer.id) -> custdist,
ORDER BY
    custdist desc,
    c_count desc
;```
## Symptom

`count(fact.x ? cond) by dim.key`, where the fact is **partial** for `dim.key` (some dim entities have no fact rows), drops the zero-match dim entities instead of reporting them with count 0. TPC-H q13: 1000/31-row result vs reference 1500/32 (the `(0, 500)` bucket — customers with no orders — disappears).

Affects **every null-propagating operator** in the `?` filter: `=`, `!=`, `is not null`, `not like`, `not ilike`, `like`, `in`, `not in`. The lone exception is `not (x like y)` (parenthesised boolean NOT), which is why q13 passed before `not like` was promoted to a first-class operator this cycle — see [[project_not_like_infix]].

## Root cause (verified empirically at sf=0.01)

The `?` filter is lowered to a **WHERE on the joined-grain CTE**, i.e. it runs *after* the `customer LEFT OUTER JOIN orders` that preserves all customers. With a null-propagating predicate, the LEFT join's NULL-padded (no-order) rows fail the WHERE and are dropped:

```sql
-- count(order.id ? order.comment not like '%x%') by order.customer.id
SELECT c_custkey, coalesce(count(o_orderkey),0) AS bucket
FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey
WHERE o_orders.o_comment not like '%x%'        -- NULL-padded rows: NULL → dropped
GROUP BY 1
```

The join type is **not** the cause. With `CONFIG.optimizations.upgrade_condition_joins = False` the join stays LEFT and the result is *still* 1000 — the null-rejecting WHERE alone drops the rows. (`UpgradeJoinOnGuards` additionally downgrades LEFT→INNER here, but that downgrade is *valid given* the null-rejecting WHERE — same rows either way. So the join-upgrade is a red herring; do not "fix" it there.)

`not (x like y)` survives only because boolean-NOT renders null-safe — `coalesce((o_comment like '%x%'), False) = False` — so NULL-padded rows evaluate True and survive (then `count(o_orderkey)` = `count(NULL)` = 0). That is the *correct* result, reached by a side effect of NOT-rendering, not by design.

## The correct general fix

Keep the `?` filter **inside the aggregate** for the partial case, so there is no outer row-rejecting WHERE:

```sql
SELECT c_custkey, count(CASE WHEN o_comment not like '%x%' THEN o_orderkey END) AS bucket
FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey
GROUP BY 1
```

Equivalently (keeps the WHERE shape, contained edit): guard the lifted condition with the fact grain key being NULL, so LEFT-pad rows always survive while real rows still get filtered:

```sql
WHERE (o_orderkey IS NULL) OR (o_comment not like '%x%')
```

Correctness of the `OR fact_key IS NULL` form, all operators (the fact grain key `o_orderkey` is NULL **iff** the row is a LEFT pad / no fact row):
- LEFT-pad row → `fact_key IS NULL` true → survives → `count(fact_key)` = 0. ✓ (entity preserved)
- real row matching `cond` (inclusion) / failing exclusion → filtered exactly as today. ✓
- real row with a genuinely NULL filtered column → `fact_key` non-null, `cond` NULL → dropped, same as standard SQL. ✓ (this is why a blanket `coalesce(cond, True)` is wrong but `OR fact_key IS NULL` is right)

`count(CASE WHEN cond THEN x END)` is the cleaner, dialect-agnostic equivalent and is preferred.

## Where to intervene

Scope strictly to: an **aggregate-`?` filter** (a `BuildFilterItem` condition feeding a `BuildAggregateWrapper`) whose filtered source is **partial** for the aggregate's GROUP grain (the dim is preserved by an OUTER join). Do **not** touch genuine top-level `WHERE` filters (those *should* reject zero-match entities) or filtered aggregates grouped at the fact's own grain (no partial dim → current WHERE lowering is fine and produces cleaner SQL).

Lowering path: `count(order.id ? cond) by order.customer.id` → a `FilterNode` (`v4_node_generators/filter.py`) materialises `order.id ? cond`, then `gen_aggregate` (`v4_node_generators/aggregate.py`) builds a `GroupNode`. The `?` condition becomes the node/CTE `condition` (see `processing/nodes/group_node.py`, `condition=self.conditions`). The detection of "source partial w.r.t. group grain" mirrors `join_upgrade._partial_addresses` / `_blocked_partials` (a concept the source only partially covers, preserved via outer join). The fact grain key for the `OR ... IS NULL` form is the aggregate source's grain key.

Two viable implementation seams:
1. **Render `?`-on-aggregate as inline `CASE`** when the aggregate is grouped over a partial dim, instead of pushing the FilterItem condition up as a node WHERE. (Preferred — fully correct, no fact-key plumbing.)
2. **Augment the lifted node condition** with `OR <fact grain key> IS NULL` when that condition sits above an OUTER join whose nullable side owns the condition's concepts. (More contained, keeps WHERE shape.)

## Acceptance

`tests/optimization/test_not_like_partial_join.py` — remove the `xfail`; both `not like` and `not (like)` must return `{(1,1),(2,1),(3,0)}`. Add `=`, `!=`, `is not null` variants asserting the zero-match entity is preserved. Full regression: `optimization/`, planner/generator suites, and `tests/modeling/tpc_h` + `tpc_ds` query suites (this changes filtered-aggregate SQL for the partial case — watch for shifts). Then `ruff/mypy/black`.

## Risk

Medium-high: filtered-aggregate lowering is core and widely exercised. The mitigation is the strict scoping above (partial-dim-grouped aggregate-`?` only) plus the full query-suite sweep. Validate that non-partial filtered aggregates (the common case) render unchanged.

Related: [[project_global_aggregate_having_collapse]] (HAVING grain — different), [[project_tpch_eval_scores_against_pragma]] (q13 reference = PRAGMA tpch(13) = 1500/32).
