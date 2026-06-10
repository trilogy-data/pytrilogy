# Handoff — v4 root-splitting / covering-union over condition-partitioned partials

Self-contained brief for the open follow-up surfaced while unifying v4 sourcing
eligibility (see `_datasource_materializes` in
`trilogy/core/processing/concept_strategies_v4.py`, and memory
`project_v4_persist_unnest_reuse`). Read top to bottom.

## The gap, in one sentence

When a concept's full population is spread across several **`complete where`-
partitioned partial datasources** (each holds only the rows where its predicate
holds) and no single complete source covers the query, v4 does not stitch them
into a covering UNION — so a query broader than any one partition fails to source.

## Confirmed repro (no test yet — distill one)

```trilogy
key customer_id int;
property customer_id.region string;
property customer_id.revenue float;
auto total_revenue <- sum(revenue);

datasource east_summary (customer_id: customer_id, total_revenue: total_revenue)
grain (customer_id) complete where region = 'east'
query '''select 101 as customer_id, 10.0 as total_revenue''';

datasource west_summary (customer_id: customer_id, total_revenue: total_revenue)
grain (customer_id) complete where region = 'west'
query '''select 202 as customer_id, 20.0 as total_revenue''';

SELECT customer_id, total_revenue ORDER BY customer_id;   -- wants BOTH partitions
```

Observed (run with `CONFIG.use_v4_discovery=True` vs default):
- **Correct**: `[(101, 10.0), (202, 20.0)]` — union of the two partitions.
- **v4**: `NoDatasourceException: No datasource exists for root concept
  local.revenue@Grain<local.customer_id>` — refuses each partial summary
  (rightly — neither covers the unfiltered population), then tries to derive
  `total_revenue` from `revenue`, which has no base source → crash.
- **v3**: `[(101, 10.0)]` — silently returns ONE partition. Wrong, and quiet.

Add a base `revenue` table and v4 correctly derives from base (both rows); the gap
only bites when the partitioned partials are the *only* coverage.

## Why it happens / how it connects to the eligibility work

`_datasource_materializes` (the unified eligibility predicate) now correctly
**rejects** a `complete where X` summary unless the query implies `X`
(`condition_implies(where, ds.non_partial_for)` — the population check). That is the
right call for a *single* source: an unfiltered query must not read a partition-
only table. The eligibility fix turned v3's silent-wrong into v4's honest error —
an improvement, not a regression (full suite stayed 796-pass incl.
`tests/complex/test_dataset_merge.py`).

But "reject each partial individually" leaves a hole: the planner never asks
"can a SET of partials jointly cover the population?" That joint-coverage step is
**root-splitting** — split one logical root into per-partition scans + UNION.

## What already exists (reuse, don't reinvent)

There IS covering-union machinery, but it's scoped to the wrong trigger:
- `_best_enum_union` in
  `trilogy/core/processing/node_generators/select_helpers/datasource_injection.py:73`
  — finds minimal covering combos of partial datasources, but only for an
  **EnumType-typed partition KEY** (e.g. a `channel` enum). It groups by
  `_extract_enum_value_for_key(ds.non_partial_for, merge_key.address)` and unions
  one source per enum value. A `complete where region = 'east'` on a plain string
  property is **not** an enum key, so it never triggers.
- `_plan_complete_where_source` in
  `trilogy/core/processing/v4_helper/source_planning.py:773` — pins a *single*
  partial datasource when the query implies its predicate. No multi-source union.
- `filter_union_children` / condition routing in
  `select_helpers/condition_routing.py` and `node_merge_node.py` — the UNION-branch
  plumbing once branches exist.
- `condition_implies` (`condition_utility.py`) — the coverage primitive.

## The shape of a fix (design sketch, not prescriptive)

Teach source-planning to attempt a **covering union over `complete where`
partials** as a sourcing strategy for a concept, parallel to `_best_enum_union`
but keyed on predicate-coverage rather than enum value:
1. Collect partial datasources binding the concept (canonical match, via the same
   column-canonical rule as `_datasource_materializes`).
2. Find a minimal subset whose `non_partial_for` predicates are **jointly
   exhaustive** for the query (their disjunction covers the queried population —
   for an unfiltered query, covers "all rows"; with a filter, covers the filtered
   slice). This is the hard part: proving exhaustiveness of arbitrary predicates is
   undecidable in general, so scope it — e.g. a known partition column with a
   closed value set (mirroring the enum approach but for a value-enumerable
   property), or an explicit `else`/complementary-predicate convention.
3. Emit a UNION-ALL of per-partition scans as the root, each scan carrying its
   partition predicate as `preexisting_conditions`.

Likely the cleanest landing is to generalize `_best_enum_union` to accept a
predicate-partitioned key (not just `EnumType`), feeding the result through the
existing union-branch path.

Decide the **safety fallback** explicitly: if joint coverage can't be *proven*,
v4 must NOT silently pick one partition (v3's bug). Either derive from base (if a
complete source exists) or raise the current `NoDatasourceException`. Surfacing a
clear "partitioned sources don't provably cover this query" error is acceptable.

## Must-pass / guardrails

1. The repro above returns both rows under v4 (and ideally v3, though v3 is out of
   scope — it's a separate planner).
2. **No silent-wrong**: a query that the partitions can't provably cover must error
   or fall back to a complete source, never return a single partition.
3. `tests/complex/test_dataset_merge.py` (the enum-partitioned union case) and the
   `complete where` tests in `tests/core/processing/test_v4_agg_source_planning.py`
   stay green — don't regress the enum path while generalizing it.
4. Re-run the eligibility unit suite `tests/core/processing/test_v4_materialized_roots.py`
   and the broad v4 sweep (`tests/engine tests/complex tests/persistence
   tests/modeling/hackernews tests/core/processing`, `-m "not adventureworks_execution"`,
   ignore `test_clickhouse_server.py`) — expect only the 3 baseline fails
   (`test_composite_rollup_aggregate_keeps_group_by`,
   `test_rowset_query_scoped_join_conflicting_filter`,
   `test_aliased_aggregate_referenced_in_having_and_order_by`).
5. Distill a parity case into `local_scripts/v4_evals/cases/` once it works — but
   note the harness only checks the FINAL select's rows on a fresh in-memory DuckDB
   (no physical persist), so inline `complete where` summaries with literal
   `query '''...'''` rows are the way to express it there.

## Status

Open. Not started beyond this characterization. Low urgency (no known real query
hits it; both planners already mishandle it, v4 at least loudly). The eligibility
refactor it descends from is shipped and green.
