---
name: project_crossrowset_inner_join_grainless_scalar
description: cross-rowset INNER join to a grainless global scalar errors ("rewrite as union") instead of broadcasting; misleading guidance drove q14 agent thrash
metadata:
  type: project
---

OPEN (handoff written). Relating a grainless single-row scalar (global `avg`/ratio-of-aggregates rowset output) to another rowset via INNER join is a legitimate BROADCAST (one row → `1=1` cross join). The engine instead raises "Cannot resolve cross-rowset INNER join ... the collapse dropped <scalar>, silently losing the intersection. Rewrite the intersection as a `union(...)`" — union is nonsensical for a scalar and misled the q14 agent (run 20260701-033309).

The broadcast WORKS in the simple case: `with per_brand as select brand, sum(amt) as tot; rowset g <- select avg(amt) by * as gavg; select per_brand.brand, per_brand.tot inner join per_brand.tot = g.gavg;` → OK. The q14 form trips the q38-family guard `_validate_cross_rowset_inner_joins` (rowset_node.py) only because BOTH operands are multi-column rowsets AND the projection includes the grainless scalar → collapse-detection fires.

Fix: (1) exempt grainless single-row scalar operands from the intersect guard / broadcast them (reuse `_is_single_row_rowset_scalar` incl. the BASIC-recursion fix from [[project_rollup_having_crossrowset_drops_subtotals]]); (2) when the guard fires with a scalar operand, advise "reference the scalar directly in where/having — global scalars broadcast, no join" NOT "rewrite as union". Idiomatic form: `where per_brand.tot > g.gavg` (works). Handoff: `evals/tpcds_agent/bug_crossrowset_inner_join_grainless_scalar.md`. Family: [[project_q38_cross_rowset_inner_join_intersect_sentinel]].
