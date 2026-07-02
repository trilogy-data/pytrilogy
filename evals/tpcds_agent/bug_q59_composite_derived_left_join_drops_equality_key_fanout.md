# BUG: composite scoped LEFT/FULL rowset join (plain-eq + derived key) DROPS the equality key → cross-product fan-out

**Classification:** framework bug — **SILENT WRONG RESULTS** (cross-product fan-out, ~6–7× row
multiplication). Surfaced by q59 ("this-year vs next-year weekly sales-price ratio per store").

**Relationship to the existing composite-mixed handoff.** This is a *distinct, more severe facet*
of the same construct documented in
[`bug_composite_mixed_key_scoped_left_join_widens_full.md`](bug_composite_mixed_key_scoped_left_join_widens_full.md):

| | that MD (`comp_mixed` matrix cell) | THIS bug (q59) |
|---|---|---|
| composite key | `agg.period+53 = fut.period AND agg.region = fut.region` | `this.store = next.store AND next.wk = this.wk + 52` |
| equality co-key sides | **distinct** addresses (`agg.region` vs `fut.region`) | **share one canonical** (`this.store` and `next.store` both pass through parent rowset `weekly_store.store_id`) |
| symptom | equality half emitted as a **separate FULL JOIN** → right-only rows leak (spurious NULLs); **no fan-out** (maxdup 1) | equality half **entirely dropped + conflated to one column** → **cross-product fan-out** (maxdup 6–7) |

So: when the two sides of the plain-equality co-key resolve (via passthrough) to the **same
underlying canonical concept**, the planner treats them as "already the same column," emits *no*
join condition for them, and the physical join carries **only the derived key** — every left row
matches every right row that shares the derived-key value (i.e. all stores in the target week).

**Language-change impact (critical, new since the other MD was written).** Query-scoped
`inner join` was **removed from the language on 2026-07-01** (see
`project_scoped_inner_join_removed`). The other MD's trigger matrix listed `inner` as the
correct-behavior escape hatch. That escape hatch is **gone** — only `left`/`full` remain, and
**both fan out** for this shape. There is currently **no correct authoring form** for
"intersect two child rowsets of a common parent on a shared plain key + a derived key."

**Status:** reproduced + root-caused to the locus. NOT fixed. Hand off to executor.

---

## Minimal repro (reliable, against the eval TPC-DS enriched model)

Workspace: `evals/tpcds_agent/results/20260702-031824/workspace` (has `tpcds.duckdb` + `raw/`).

```python
import sys; sys.path.insert(0, 'evals')
from pathlib import Path
from common import scoring
ws = Path('evals/tpcds_agent/results/20260702-031824/workspace')
eng = scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(BODY)[-1]           # BODY below
rows = list(eng.execute_raw_sql(sql).fetchall())
```

```trilogy
import raw.store_sales as ss;
with weekly_store as
where ss.date.year = 2001 or ss.date.year = 2002
select ss.store.id as store_id, ss.date.week_seq as week_seq, ss.date.year as year,
       sum(ss.sales_price) as tot ;
with this_year as
where weekly_store.year = 2001
select weekly_store.store_id, weekly_store.week_seq, weekly_store.tot ;
with next_year as
where weekly_store.year = 2002
select weekly_store.store_id, weekly_store.week_seq, weekly_store.tot ;
left join this_year.store_id = next_year.store_id
  and next_year.week_seq = this_year.week_seq + 52
select this_year.store_id, this_year.week_seq,
       this_year.tot / next_year.tot as ratio
order by this_year.store_id, this_year.week_seq limit 200;
```

Observed: `distinct_keys=29 of 200 rows`, **maxdup=7** per `(store, week_seq)`. Correct answer is
one row per `(store, week_seq)`.

## Trigger matrix (toggle one ingredient)

| join key | join type | rows | fan-out? | rendered join ON |
|---|---|---|---|---|
| `store = store AND next.wk = this.wk + 52` (plain-eq + derived) | `left` | fan-out | ❌ **YES (7×)** | `next.wk = this.wk+52` **only** — store dropped |
| same | `full` | fan-out | ❌ **YES (7×)** | derived key only |
| `store = store AND next.wk = this.wk` (both plain) | `left` | 1:1 | ✅ no | `store = store AND wk = wk` (both keys, correct) |
| `next.wk = this.wk + 52` (derived only) | `left` | fan-out | (expected) | derived key only |

The composite plain-eq+derived case is **byte-identical** in behavior to the derived-only case →
proof the equality co-key is fully dropped. Both-plain composite is correct.

## Smoking gun in generated SQL

```sql
-- this_year.store_id is sourced from the NEXT_YEAR side (canonical conflation):
SELECT "questionable"."next_year_weekly_store_store_id" as "this_year_weekly_store_store_id", ...
FROM "questionable"
  LEFT OUTER JOIN "juicy"
    ON "questionable"."next_year_weekly_store_week_seq" = "juicy"."_virt_func_add_..."
```

The equality `store = store` appears **nowhere** in an ON clause; the output `this_year.store_id`
is aliased straight off `next_year`'s column. Contrast the both-plain control, which renders
`... ON next.store = this.store AND next.wk = this.wk` (both keys present, no fan-out).

## Root cause (locus)

The plain-equality co-key and the derived-expr key of a *single* composite scoped join flow
through *different* resolution paths. When both sides of the plain-equality co-key share a
canonical address (child rowsets of a common parent), the shared-canonical inference in
`get_node_joins` decides the column is "already unified" and emits no ON condition for it, so the
inferred join carries only the derived key's `_virt_func_add` pseudonym → cross-product.

The `_enrich_via_derived_join_key` co-key materialization (added 2026-06-30, see
`project_composite_derived_join_drops_equality_key`) sources `scoped_full_join_keys |
scoped_left_anchor_keys` co-keys *specifically to keep both sides exposing the co-key so the
inferred join carries it* — but it does not cover this shared-canonical two-independent-rowset
shape (the join is resolved by the standard cross-rowset path, not that enrich MergeNode), so the
co-key is still dropped.

**File pointers**
- `trilogy/core/processing/join_resolution.py` — `get_node_joins` / `resolve_join_order_v2`:
  shared-canonical key grouping that omits an ON condition when both sides share a canonical, even
  though the *authored* scoped join demands that equality; and the derived-key CTE boundary split.
- `trilogy/core/models/build.py` — `_build_scoped_merge_index`, `scoped_left_anchor_keys`,
  `scoped_full_join_keys`, `scoped_partial_derived` (how composite keys register).
- `trilogy/core/processing/node_generators/rowset_node.py:402` `_enrich_via_derived_join_key` —
  co-key sourcing (lines ~440–473) does not reach this shared-canonical cross-rowset shape.

## Fix direction

A single composite scoped join must resolve as ONE join with a combined ON clause carrying **every
authored key** and one directionality, regardless of (a) plain vs derived key, or (b) whether the
plain-equality co-key's two sides share a canonical address. A shared-canonical equality co-key
inside an *explicit authored scoped join* must NOT be silently elided — the user asked for
`store = store` as a join condition; conflating the columns and dropping the ON is the defect.
The both-plain composite-LEFT path (`scoped_left_anchor_keys`, q78) is the correct-behavior
reference.

## Guardrails (must not regress)

- `tests/test_scoped_derived_rowset_join_matrix.py` — the derived-rowset-join matrix; single-key
  derived joins and both-plain composite LEFT must stay correct. The existing
  `test_composite_mixed_key_left_join_should_not_widen` xfail (distinct-address widen facet) should
  flip to pass alongside this fix.
- q78 (`scoped_left_join_multi_partial_anchor`), q29 (nullable inner not widened to full).
- Add a fan-out guard for the **shared-canonical** variant: a 3-rowset chain (parent rowset + two
  year-filtered child rowsets, both projecting the parent key) joined on `child.key =
  child.key AND derived`. NOTE: a naïve row-keyed synthetic model of this shape currently hits a
  RecursionError (a *separate*, related failure — see
  `bug_left_derived_rowset_join_recursion.md`), so build the guard against a grain-keyed /
  conformed-dimension model like the eval `raw` model rather than a bare `property row_id.k` model.

## Also worth flagging

The same q59 authoring shape produces **two different framework failures** depending on the model:
cross-product fan-out on the conformed TPC-DS model (this bug) vs `RecursionError` on a simple
row-keyed synthetic model. Both stem from composite plain+derived scoped rowset joins over a
shared-parent chain; a fix should target both.
