# q02 — derived-key union-join between rowsets + base-model WHERE → unbounded recursion

> **FIXED 2026-07-05.** Two root causes, neither a recursion guard:
> 1. `_enrich_rowset_node` now PARTITIONS the unsatisfied set (`_rowset_scope_routed`):
>    the derived-key enrichment only bundles concepts the cross-rowset relation can
>    resolve; base-model WHERE args are never delegated to the other rowset (the
>    mutual-delegation cycle), and conditions containing base residue are withheld
>    from that call so the merge makes no false preexisting claim.
> 2. Condition validation no longer treats the scalar/rowset EXEMPTIONS as
>    SATISFACTION: at a final (depth-0) scope at least one node must actually apply
>    the condition (`_stack_applies_condition`), and loop completion only advertises
>    `preexisting_conditions` when some parent applied it. This also fixed an
>    adjacent SILENT WRONG-RESULTS bug: `where <base concept>` over a pure-rowset
>    select (any join type, even a single bare rowset) silently dropped the filter.
>    Translation RowsetNodes now advertise their body WHERE so an outer condition
>    the body implies (q44's redundant `where store.id = 1`) counts as applied.
>
> D2/D4 now raise a clean `DisconnectedConceptsException` with join/merge guidance
> (matching the pre-existing LEFT behavior); D/D3 unchanged. Guards:
> `tests/test_rowset_join_base_where_matrix.py` (join × key × where sweep + filter-
> effectiveness cells) and `tests/core/processing/test_discovery_validation.py`.

Run: `evals/tpcds_agent/results/20260705-200535` (q02 burned 989,990 tok / 30 calls, eventually PASS via a window-`lead` rewrite).

## Symptom
Agent naturally reached for the idiom the syntax examples advertise (agent-info example (4):
"expression join keys … `union join` on `week_seq + 53`"). Two independent rowsets aggregating
web+catalog sales by `(week_seq, day_of_week)`, joined `union join cur.week_seq + 53 = fut.week_seq`
(+ optional plain `day_of_week` co-key), with a final `where s.date.year = 2001`.

Engine raised `RecursionError` internally → CLI reported `Resolution error in query02.preql: query
could not be planned; this is a bug.` (TWICE — msgs 18/19 and 28/29 in the conversation). The
`Syntax [101]: Using FROM keyword?` (msg 43) is a *separate*, genuine agent error (`… from cur`)
with a correct, clear message — not a framework bug.

The two rowset-join attempts are what drove the token churn: the opaque "this is a bug" gave the
agent no signal, so it abandoned the (correct) union-join idiom and eventually stumbled onto a
window-`lead` rewrite.

## Minimal repro (RecursionError)
```
import raw.all_sales as s;
rowset cur_period <- where s.channel in ('WEB','CATALOG')
  select s.date.week_seq, sum(s.ext_sales_price) as sales_amt;
rowset fut_period <- where s.channel in ('WEB','CATALOG')
  select s.date.week_seq, sum(s.ext_sales_price) as sales_amt;

where s.date.year = 2001                                  -- base-model WHERE arg
select cur_period.week_seq,
       round(cur_period.sales_amt / fut_period.sales_amt, 2) as r,
union join cur_period.week_seq + 53 = fut_period.week_seq; -- DERIVED key
```

## Trigger matrix (HEAD = the two rowsets above)
| # | join | key | final WHERE | result |
|---|------|-----|-------------|--------|
| A | union  | derived `+53` single | none | OK |
| B | union  | plain single         | none | OK |
| C | union  | plain composite      | none | OK |
| D | union  | derived+plain composite | none | OK |
| E | subset | derived single       | none | OK |
| F | left   | derived single       | none | OK |
| D2 | union | derived+plain        | `where s.date.year=2001` (base) | **RecursionError** |
| D3 | union | derived+plain        | `where cur_period.week_seq>5000` (rowset output) | OK |
| D4 | union | derived single       | `where s.date.year=2001` (base) | **RecursionError** |

Both agent attempts = D2/D4 shape. Two toggles are jointly necessary:
1. a **derived** join key between the two rowsets (`+53`; plain-only never recurses), AND
2. a final `where` referencing a **base-model** concept NOT output by either rowset
   (`s.date.year`). A WHERE on a rowset output (D3) or no WHERE (D) is fine.

## Root cause  `trilogy/core/processing/node_generators/rowset_node.py`
Recursion cycle (from the captured stack):
`_enrich_rowset_node:806` → `_enrich_via_derived_join_key:686` → `source_concepts(...)` →
`_generate_rowset_node` → `gen_rowset_node:949` → `_enrich_rowset_node:806` → … (unbounded).

- `_enrich_rowset_node` (line 781-793) builds `cond_remaining` = WHERE row-args the node can't
  produce and that aren't ROWSET-derived. The base-model arg `s.date.year` lands here.
- Line 808 passes `unique(remaining + cond_remaining)` into `_enrich_via_derived_join_key`.
- Inside `_enrich_via_derived_join_key` (line 686) that list is sourced together with the OTHER
  rowset's join key (`other_keys`): `source_concepts(mandatory_list = enrich_remaining + other_keys
  + co_keys, …)`. Sourcing a base-model concept **alongside** the other side's key re-enters the
  other rowset's generation, which re-runs `_enrich_rowset_node` → finds the same derived key →
  calls `_enrich_via_derived_join_key` again with the same base arg → loops.

The function's own docstring (lines 648-650) asserts "the other rowset is never re-sourced through
this one (which would recurse)", and the `_producible_derived_join_keys` gate (added 2026-06-30 for
the LEFT case) is meant to prevent it — but that gate only inspects the *derived-key pairing's*
producibility. It does NOT account for a base-model WHERE arg (`cond_remaining`) being bundled into
the same `source_concepts` call, which independently pulls the base fact through the other rowset.
There is no visited-set / recursion guard, and `history` doesn't dedupe because each re-entry grows
the `mandatory_list` (adds keys), so every request is nominally distinct.

## Classification: NEW distinct trigger, same recursion family
Same code path and failure mode as the OPEN memory note
`project_left_derived_rowset_join_recursion.md` (rowset_node.py `_enrich_via_derived_join_key`
re-sourcing the other rowset). But:
- that note is **LEFT**-specific and states "INNER + plain-eq-LEFT both fine";
- the union-join-between-rowsets collapse was "FIXED 2026-07-04"
  (`project_union_join_between_rowsets_collapse_fixed.md`) — but only for the **no-base-WHERE** case.

Here the join is **UNION** and the trigger is the **base-model WHERE arg (`cond_remaining`)**, not
LEFT re-sourcing. So: a NEW, distinct trigger of the known unguarded-derived-key-enrichment
recursion — not a regression (D/D3 paths work), and not merely a residual of the LEFT note.

## Canonical answer builds on current engine — YES
`tests/modeling/tpc_ds_duckdb/query02.{sql,preql}` uses a window `lead(amt, 53) over (order by
week_seq)` (no rowset join), builds to 1 statement and executes → 53 rows
`[(5270, 3.52, 2.2, 1.74, 1.6, 3.01, 3.89, 3.47), …]`. The agent's final passing
`workspace/query02.preql` is the same window-`lead` shape and also builds. The framework obstacle is
confined to the (idiomatic, example-advertised) `union join` rowset approach.

## Fix direction (do NOT implement here)
Either (a) exclude base-model `cond_remaining` args from the `_enrich_via_derived_join_key`
`source_concepts` mandatory list (source/apply them on the merge output instead of bundling them
with the other rowset's key), or (b) add a real visited/recursion guard on the derived-key
enrichment path (address-set memo across `source_concepts` re-entry). Minimum: catch the
`RecursionError` and surface an actionable planner error instead of "this is a bug".
