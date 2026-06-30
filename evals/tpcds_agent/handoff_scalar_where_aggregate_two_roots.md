# Handoff: scalar WHERE-aggregate gate needs a distinct unfiltered root

**Status:** designed + validated, NOT implemented. Sizable planner change.
**Date:** 2026-06-29

## What this is

Trilogy's design intent (documented in `trilogy/ai/constants.py:93,112`): **an aggregate
used as a filter in WHERE is computed over the WHERE-*unfiltered* universe**, while the
SELECT/HAVING aggregates are filtered by WHERE. The WHERE-agg is still at the select grain
— just unfiltered input.

This already works for **grouped** selects and is shipped. It does **not** work for a
**scalar** (grainless) select where the WHERE-agg is also (or equals) a SELECT output, e.g.

```
select sum(cost) -> v where x > 1 and v > 1000;
```

That shape is currently **blocked by a clean parse-time guard** (see "Current state"), so no
user hits wrong results — but full support is wanted.

## Already done & committed (do NOT redo)

1. **Grouped WHERE-agg allowed** — `select_finalize.py` `_validate_syntax` only raises the
   "Cannot reference an aggregate derived in the select … move to HAVING" guard when
   `not select.grain.components` (scalar). Grouped computes the WHERE-agg at select grain
   over the unfiltered universe correctly. (+ author.py mirror.)
   See [[project_where_aggregate_guard_relaxed_grouped]].
2. **`_is_scalar_only` condition guard** — `discovery_validation.py`: a single-row scalar
   node is no longer exempted from a WHERE that filters its *own* output. Fixed a reachable
   silent-drop bug (`where grand_total > 5000 select grand_total` returned 2160 instead of
   `[]`). Threaded `condition` through 4 call sites (discovery_validation + concept_strategies_v3).
   See [[project_is_scalar_only_condition_guard]].

Baseline suite with these: **4697 passed** (only 3 unrelated failures from another agent's
`constants.py` edits: ai/test_prompt ×2, scripts/test_agent_info).

## Current state of the scalar case (the guard)

`select_finalize.py` `_validate_syntax`, ~line 1556:
```python
if select.where_clause and not select.grain.components:
    # raises "Cannot reference an aggregate derived in the select … move to HAVING"
    # for a locally-derived agg alias in WHERE; also _validate_where_aggregate_matches_select
```
Keep this guard as the fallback until the fix below lands; only remove it for shapes the
fix actually handles.

## The target SQL (validated against data)

Data: `x∈{1,2,3,4}`, `cost={100,50,2000,10}`; `sum(all)=2160`, `sum(x>1)=2060`.

```sql
WITH
filtered AS (SELECT sum(cost) AS v FROM t WHERE x > 1),   -- output root: FILTERED
gate     AS (SELECT sum(cost) AS g FROM t)                -- gate root: UNFILTERED
SELECT filtered.v
FROM filtered
INNER JOIN gate ON gate.g > 1000                          -- gate condition = JOIN predicate
```
Verified: `>1000`→`2060`; `>5000`→`[]` (no NULL); `>2100`→`2060` (proves gate sees the
**unfiltered** 2160, not 2060). `ON gate.g > 1000` ≡ gate emits
`CASE WHEN sum(cost)>1000 THEN 1 END AS keep` joined `ON keep IS NOT NULL`.

### Target node shape
```
MergeNode  [join ON g > N]
 ├─ GroupNode  v = sum(cost)   filter: x>1     ← root A: output, FILTERED
 └─ GroupNode  g = sum(cost)   filter: none    ← root B: gate,  UNFILTERED
```

### Current (broken) node shape
```
GroupNode  v = sum(cost)
 └─ MergeNode  [cond: x>1 AND _virt>N]
     ├─ GroupNode  _virt = sum(cost)            ← gate
     └─ SelectNode cost,x  WHERE x>1            ← ONE SHARED base, already filtered
```

## Root cause — two gaps

**Gap #1 (the core, must break): one shared, pre-filtered base instead of two roots.**
The planner sees `v` and the gate aggregate are both `sum(cost)` at the same (global) grain,
so it sources them from a *single* base scan and pushes `x>1` onto that shared scan → the
gate reads filtered data (2060, wrong; should be 2160). **The unfiltered gate root must get
a different canonical concept + build path** so it is NOT deduped/shared with the filtered
output's source. The row filter must scope to root A's aggregation only, leaving root B
reading the raw source.

**Gap #2: gate condition placed as a node predicate (→ HAVING / `1=1`) instead of a join.**
`_virt > N` is attached to the gate node and pushed into its CTE as HAVING, which empties it;
the consumer then joins `1=1` and the outer `sum()` over the empty join returns a **NULL row**
instead of zero rows. Target: the gate condition is the **join predicate** between roots A
and B (per-user: "join on `<gate> IS NOT NULL`, not `1=1`"). Gap #2 likely falls out once
there are genuinely two roots to join. Do NOT solve this by pushing to HAVING (user explicit).

How grouped already gets it right (reference implementation — study its SQL):
`select x, sum(z) by x as sx where f=1 and sx>5` →
- `quizzical` (base, **unfiltered**), `wakeful` (gate: `GROUP BY x HAVING sx>5` over unfiltered
  base), `highfalutin` (output arm: `WHERE f=1 GROUP BY x`), joined `ON x`. The row filter is
  on the output arm; the join is on the **grain key**, so a failed group is dropped (no NULL).
  Scalar has no grain key → use the **gate condition as the join predicate** instead.

## What was tried and is INSUFFICIENT (don't repeat)

- **Differentiation = rewrite the WHERE alias ref to its lineage** (so build mints a distinct
  `_virt_agg_*`). Prototype `_differentiate_where_aggregate_outputs` in select_finalize
  (reverted). Result: output filtered (2060) ✓, but **gate still FILTERED (2060)** because
  `x>1` is still pushed to the shared base, AND **NULL on gate-fail**. So a distinct concept
  at the *condition* level is not enough — the distinct concept must also force a distinct,
  *unfiltered* build/source path (Gap #1).
- **Inline FILTER on the output** (`sum(cost ? x>1)` + gate as HAVING): correct results in a
  monkeypatch prototype, but requires rebuilding the committed output concept lineage in the
  v2 staging flow, and the user rejected the inline approach as unnecessary — the grouped
  two-root structure proves inline isn't needed.
- **`by *` in HAVING** for the gate: respects the WHERE filter (computes 2060), so it does
  NOT yield an unfiltered gate. Dead end.
- **Auto-route the WHERE-agg to HAVING**: rejected by user — breaks the unfiltered-gate design
  intent (HAVING is post-filter).

## Where to dig (code map)

- `parsing/common.py:164-185` — inline aggregate in a condition → fresh `_virt_agg_*` virtual
  concept (the differentiation that DOES happen for inline). `:215` — an alias `ConceptRef`
  returns the canonical concept (no differentiation → the dedup origin).
- `core/processing/concept_strategies_v3.py`:
  - `initialize_loop_context` (~167) — `required_filters` / `must_evaluate_condition_on_this_level_not_push_down`;
    line ~180 excludes `AGGREGATE + SINGLE_ROW` from forcing local eval (relevant to scalar).
  - `generate_loop_completion` (~370) / `_restrict_completion_conditions` (~327) — decide
    `condition_required` and which atoms re-apply at the merge; where the gate atom currently
    rides.
- `core/processing/node_generators/group_node.py`:
  - `_resolve_parent_sources` (~338) → `_source_parent_concepts(..., conditions=...)` — passes
    the FULL where to the parent base scan (this is where `x>1` lands on the shared base).
  - `_build_group_node` (~498) — sets `preexisting_conditions = conditions.conditional`
    unconditionally; `_group_conditions_to_apply` (~485) applies the whole condition or nothing.
- `core/processing/discovery_validation.py` — `_is_scalar_only` (now condition-aware),
  `_is_independent_scope`.
- `dialect/base.py` ~632 — HAVING rendering; join rendering for the final cross-join.

The crux of Gap #1 is making the WHERE-agg source as a **separate, unfiltered** aggregation
root rather than being merged onto the filtered output's base. That likely means: when a
WHERE condition for a (scalar) select contains an aggregate atom, source the output with the
row-atoms as its condition, and source the WHERE-agg(s) as independent unfiltered concepts,
then combine via a MergeNode whose **join condition is the aggregate atom** — mirroring the
grouped arms but keyless.

## Test cases / validation harness

Discriminating dataset + expected (scalar):
| query | expect | proves |
|---|---|---|
| `select sum(cost)->v where x>1 and v>1000` | `2060` | output filtered, gate passes |
| `… where x>1 and v>5000` | `[]` | no NULL on gate-fail |
| `… where x>1 and v>2100` | `2060` | gate is UNFILTERED (2160>2100), not filtered (2060) |
| `… where v>1000` (no row filter) | `2160` | pure gate |
| `where grand_total>5000 select grand_total` | `[]` | already fixed by `_is_scalar_only` |

Add a generate+execute test alongside `tests/test_derived_concepts.py` (where the current
grouped/scalar tests live). The `_is_scalar_only` regression test is
`test_where_filter_on_scalar_output_value_is_applied`.

Don't regress the grouped behavior — `test_where_aggregate_input_not_filtered_by_where`,
`test_where_aggregate_matching_select_output_executes`, `test_where_aggregate_on_grouped_select_executes`.

## Gotchas

- Shared working tree: another agent actively edits `trilogy/core/models/build.py`. If the
  full suite shows a cascade of `AttributeError: 'Function' object has no attribute 'is_aggregate'`,
  that's theirs, not yours. To get a clean signal without touching their file, run with a
  scratchpad `sitecustomize.py` on `PYTHONPATH` that patches `Factory._is_bare_aggregate`.
- NEVER `git stash` (parallel agents share the tree).
- Default backend is PEST → `select_finalize.py` path; `author.py` `validate_syntax` is the
  legacy path and does NOT run for duckdb execution (verified) but keep it consistent.
