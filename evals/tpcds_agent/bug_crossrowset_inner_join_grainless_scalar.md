# BUG: cross-rowset INNER join to a grainless global scalar errors ("rewrite as union") instead of broadcasting

**Classification:** framework bug — spurious rejection + misleading guidance. Relating a grainless
single-row scalar (e.g. a global `avg` / ratio-of-aggregates rowset output) to another rowset via
an INNER join is a legitimate **broadcast** (the scalar is one row → cross-join / `1=1`). The engine
instead raises a cross-rowset "intersection" error and advises rewriting as `union(...)`, which is
nonsensical for a scalar and sent the q14 agent down a rabbit hole.

**Status:** confirmed, minimal repro showing the broadcast SHOULD (and partly does) work. NOT fixed.

## Symptom (from q14, run 20260701-033309)

```
Resolution error: Cannot resolve cross-rowset INNER join ch.s.channel = overall_stats.overall_avg:
it intersects two independent rowsets but the collapse dropped overall_stats.overall_avg, silently
losing the intersection. Rewrite the intersection as a `union(...)` of the arms.
```

The agent wanted to attach a global average to each channel row to compare against it. It reached
for a join (`... inner join ch.channel = overall_stats.overall_avg`) and got this error; the
`union(...)` suggestion is wrong for a scalar broadcast, so it thrashed.

## The broadcast is legitimate (it works in the simple case)

```trilogy
with per_brand as select brand, sum(amt) as tot;
rowset g <- select avg(amt) by * as gavg;

select per_brand.brand, per_brand.tot
inner join per_brand.tot = g.gavg;      -- OK: 2 rows, scalar broadcast, no error
```

So joining a rowset column to a grainless scalar resolves fine here. The q14 form trips the
**q38-family** guard (`_validate_cross_rowset_inner_joins`, see memory
`project_q38_cross_rowset_inner_join_intersect_sentinel`) only because BOTH operands are
multi-column rowsets AND the query projects the grainless-scalar operand, so the collapse-detection
fires — even though one operand is a single global row that should simply broadcast.

## Fix direction

Two parts:

1. **Exempt grainless single-row scalars from the intersect guard / broadcast them.** In
   `_validate_cross_rowset_inner_joins`, when one join operand resolves to a grainless single-row
   rowset scalar (or a BASIC concept derived only from such — see the sibling gap in
   `bug_rollup_having_crossrowset_drops_subtotals.md`, `_is_single_row_rowset_scalar`), treat the
   join as a broadcast attach (`1=1`) rather than a two-rowset intersection. The scalar can't be
   "dropped by the collapse" — it's one row for every output row.

2. **Fix the guidance for the residual real cases.** When the guard legitimately fires but one side
   is a grainless scalar, the message should say *"reference the scalar directly in
   `where`/`having`/the projection — a global scalar broadcasts, no join needed"*, NOT "rewrite as
   `union(...)`" (union is meaningless for a scalar broadcast and actively misled the agent).

The idiomatic form the agent should have used is the direct reference — `where per_brand.tot >
g.gavg` / `having sum(...) > g.gavg` (both resolve) — so #2's guidance is the high-value half even
before #1 lands.

## Guardrails

- Simple scalar-broadcast joins (the working repro above) must keep working.
- Genuine two-rowset INNER intersections that drop a real (multi-row) operand must still error with
  the `union(...)` suggestion (the q38 case that guard was built for).

## File pointers

- `trilogy/core/processing/node_generators/rowset_node.py` — `_validate_cross_rowset_inner_joins` (guard + message).
- Sibling: `evals/tpcds_agent/bug_rollup_having_crossrowset_drops_subtotals.md` (`_is_single_row_rowset_scalar` — the grainless-scalar detector to reuse), `project_q38_cross_rowset_inner_join_intersect_sentinel`.
