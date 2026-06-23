# Bug/handoff: HAVING on a LEFT-joined rowset's partial measure → re-rendered at outer scope → INVALID_REFERENCE_BUG

**Status:** FIXED 2026-06-23. Found 2026-06-22 (enriched q78). trilogy 0.3.285.
Fix: a HAVING scalar-derived expression (e.g. `coalesce(x, 0)`) is rewritten to
reference its matching SELECT alias before render/validation
(`_substitute_having_derived` in `trilogy/parsing/v2/select_finalize.py`, mirroring
the existing window/aggregate HAVING-substitution passes), so the renderer reads
the materialized column instead of re-deriving the inner (now-absent) argument at
the outer scope. The real trigger was HAVING on a *coalesce-derived* concept (the
INNER+coalesce form raised the same `INVALID_REFERENCE_BUG`), not the LEFT join /
partial measure per se. Unit test xfail removed; it now asserts clean SQL +
preserved `LEFT OUTER JOIN`.
**Severity:** medium-high — `generate_sql` raises `ValueError: Invalid reference
string found in query` (an `INVALID_REFERENCE_BUG` sentinel in the rendered SQL).
At agent runtime the same shape produced a **silently wrong / 0-row** result that
ran cleanly, sending the agent chasing its own tail (q78, ~2.5M tokens).

**Minimal unit test (checked in):**
`tests/test_rowset_outer_join_having_on_partial_measure.py` — two controls
(LEFT-no-HAVING, INNER+HAVING both render fine), an `xfail(strict=True)` for the
bug, and a pin on the current `ValueError`. Self-contained fixture, no DB.

## Minimal trigger (single key, two tiny models)

```trilogy
import fa as fa;   -- a_item, a_qty
import fb as fb;   -- b_item, b_qty
with agg_a as select fa.a_item as it, sum(fa.a_qty) as qa;
with agg_b as select fb.b_item as it, sum(fb.b_qty) as qb;
select agg_a.it, agg_a.qa, coalesce(agg_b.qb, 0) as qb
left join agg_a.it = agg_b.it
having coalesce(agg_b.qb, 0) > 0;          -- filtering the PARTIAL side -> corrupt SQL
```

Trigger is precisely **HAVING on the LEFT (partial) rowset's measure**:
- drop the `having` → renders fine;
- change `left join` → `inner join` (+ `having agg_b.qb > 0`) → renders fine.

## Root cause (from the rendered SQL)

The HAVING predicate is emitted **twice**:

```sql
thoughtful as (                                   -- inner CTE: CORRECT
  SELECT "quizzical"."agg_a_qa" as "agg_a_qa",
         coalesce("wakeful"."agg_b_qb",0) as "qb"
  FROM "quizzical" INNER JOIN "wakeful" on "quizzical"."agg_a_it" = "wakeful"."agg_b_it"
  WHERE coalesce("wakeful"."agg_b_qb",0) > 0      -- applied here, resolvably
)
SELECT "quizzical"."agg_a_it", "quizzical"."agg_a_qa", "thoughtful"."qb"
FROM "thoughtful" INNER JOIN "quizzical" on "thoughtful"."agg_a_qa" = "quizzical"."agg_a_qa"
WHERE coalesce(INVALID_REFERENCE_BUG,0) > 0       -- RE-APPLIED here, unresolvably
```

The outer SELECT FROMs only `thoughtful` ⋈ `quizzical`; the partial measure
`agg_b_qb` is not exposed there, so re-rendering `coalesce(agg_b.qb,0)` at the
outer scope yields `INVALID_REFERENCE_BUG`. The filter was **already applied**
correctly in `thoughtful`, so the outer re-application is both redundant and
broken. (Also note the LEFT join shows up as `INNER JOIN` inside `thoughtful` once
the partial-measure filter lands — the outer-join partiality is effectively lost.)

This is the same family as the window-in-HAVING re-render bug
(`bug_window_function_in_having.md`): a HAVING predicate over a value that is only
materialized in an upstream CTE is re-rendered at an outer scope where it can't
resolve.

## Fix direction

Apply the partial-measure HAVING predicate **once**, at the CTE where the partial
measure is materialized (`thoughtful` already does this correctly), and do **not**
re-emit it at the outer scope — or, if the outer filter must stay, project the
partial measure through so the outer reference resolves to a real column. Either
way the rendered SQL must never contain `INVALID_REFERENCE_BUG`, and the LEFT-join
semantics (keep unmatched left rows except where the filter excludes them) must be
preserved.

Likely code areas: the HAVING/condition placement + render for outer (LEFT/FULL)
scoped joins over rowsets — overlaps `trilogy/dialect/base.py` condition render and
the merge/outer-join node assembly. Compare against the INNER control (which
renders the filter once and resolves), and the no-HAVING control.

## Scope / relation

- `bug_outer_scoped_join_two_rowset_measures.md` — the **projection-only** outer
  two-rowset case (just selecting both measures, no filter) is **FIXED**
  (`c511e25a fix_join_handoff`). This bug is the still-open extension: add a HAVING
  filter on the partial measure and it breaks again.
- Part of the broader cross-rowset / cross-model relation theme (q64, q05, q29,
  q78). The cheaper agent-side mitigation for q78 specifically is to use the
  unified `all_sales` model (channel-filtered sums by a shared grain) instead of
  importing + outer-joining per-channel models — see the q05/q80 guidance handoff.

## Provenance

Enriched eval q78 (year-2000 store-vs-other-channel quantity ratio, keep store rows
where other-channel qty > 0). Reference uses `all_sales` and returns 100 rows; the
agent decomposed per channel, LEFT-joined the rowsets, filtered other-channel qty
in `having`, and got a silently-wrong 0-row result it could not reconcile with its
own probes (which proved 9229 store∩web and 30 three-way matches).
