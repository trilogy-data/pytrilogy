# q78 — 3-way scoped LEFT join silently rendered as FULL JOIN (wrong anchor)

Run: `evals/tpcds_agent/results/20260628-175514` (model deepseek-chat). q78 churned
~1.30M tokens (+145% vs 528k pre-fix) and FAILED (result differs from reference).

## Classification

**(a) Framework codegen/resolution BUG** — directional intent of a 3+-rowset scoped
LEFT join is lost; it is lowered to symmetric FULL JOINs. The query runs (exit_code 0)
but returns semantically wrong rows. The silent-but-wrong behavior is what drove the
agent to rephrase the join ~10 times.

The "disconnected" signal in the prompt is a **red herring**: the only occurrence of
"disconnected" in `agent_log.q78.jsonl` is inside the `agent-info` help text
(`...disconnected copy that will not join`). No `DisconnectedConceptsException` was
ever raised for q78. The actual errors the agent hit were `Ambiguous reference`
and `Undefined concept` on union-arm output paths (secondary friction, see bottom).

## Symptom

q78 needs: store rows (year=2000, never-returned, identified customer) **LEFT JOIN**
the combined web+catalog never-returned totals on (year, item, customer), keeping
store rows where the other-channel qty is positive. Store is the anchor and must be
preserved on every output row.

The agent's first attempt (log idx 38, run idx 42) used a 3-way chained scoped LEFT
join and the query **ran returning 386,671 rows** — but the top rows had
`store_qty = NULL` while `other_qty` was populated. A LEFT join anchored on store can
never null the store columns; the generated plan was not anchored on store at all.

## Failing construct

```preql
# three per-channel rowsets store_nr / web_nr / catalog_nr (each grouped to
# year,item,customer), then:
select
    store_nr.the_year, store_nr.item_id, store_nr.customer_id, store_nr.store_qty,
    coalesce(web_nr.web_qty,0) + coalesce(catalog_nr.catalog_qty,0) as other_qty
left join store_nr.the_year   = web_nr.the_year   = catalog_nr.the_year     # chained form
left join store_nr.item_id    = web_nr.item_id    = catalog_nr.item_id
left join store_nr.customer_id = web_nr.customer_id = catalog_nr.customer_id
having (coalesce(web_nr.web_qty,0) + coalesce(catalog_nr.catalog_qty,0)) > 0;
```

The agent also tried the equivalent **star form** (`store->web` and `store->catalog`
as separate `left join` lines). Both render identically wrong.

## Minimal repro

`scratchpad/repro_q78b.py` (read-only `generate_sql` against the run's workspace
models). Two-way vs three-way:

```python
env = Environment(working_path=WORKSPACE)          # .../20260628-175514/workspace
engine = Dialects.DUCK_DB.default_executor(environment=env)
sql = engine.generate_sql(TEXT)[-1]
```

Variant A — **2-way** `store_nr LEFT JOIN web_nr` on (year,item,customer):
```
"cooperative"(store)  LEFT OUTER JOIN  "vacuous"(web)  ON store.* = web.*      # CORRECT
```

Variant B — **3-way star**, store anchor, LEFT to web and to catalog:
```
FROM "late"(web)
FULL JOIN "cooperative"(catalog) ON web.* = catalog.*
FULL JOIN "vacuous"(store)       ON coalesce(web.*, catalog.*) = store.*       # WRONG
```
Store (the intended LEFT anchor) is joined **last via FULL JOIN**, so web-only and
catalog-only rows survive with NULL store columns → 386,671 rows instead of the
store-anchored subset.

## Root cause (file:line)

The 3-rowset merge runs through `MergeNode.generate_joins` →
`get_node_joins` → `resolve_join_order_v2` in
`trilogy/core/processing/join_resolution.py`.

A scoped LEFT join encodes its directionality only as a **partial flag on the
optional side**. For the star form, BOTH `web_nr` and `catalog_nr` are marked partial
against the shared anchor keys `store_nr.{the_year,item_id,customer_id}` (confirmed via
instrumented `resolve_join_order_v2`: `partials = {web_nr: [store keys], catalog_nr:
[store keys]}`, while `store_nr` is complete). `scoped_full_join_keys` is **empty**
(the user wrote LEFT, so `build.py:2330` / `query_processor.py:1196` add nothing).

Two interacting defects then force FULL:

1. **Join ordering does not anchor on the complete source.**
   `resolve_join_order_v2` / `_score_join_candidate`
   (`join_resolution.py:204-401`) picks `web_nr` (a *partial* source) as the join base
   and adds the *complete* `store_nr` as a right-side member. The only source that is
   the LEFT side of every scoped join (store_nr) is not made the anchor.

2. **`get_join_type` + `reduce_join_types` collapse to FULL.**
   With a partial left and a complete right, `get_join_type`
   (`join_resolution.py:109-145`, branch 141-144) yields RIGHT/LEFT_OUTER per pair;
   across the two partial base members `reduce_join_types`
   (`join_resolution.py:148-159`) sees both left- and right-preserving needs and
   returns `JoinType.FULL` (line 153-154). `ensure_content_preservation`
   (`162-197`) keeps it FULL.

Net: when ≥2 rowsets are LEFT-joined onto the SAME complete anchor, the planner
preserves all populations symmetrically (FULL) instead of preserving only the anchor
(store LEFT JOIN web LEFT JOIN catalog). The 2-way case is correct only because there
is a single partial side and no `reduce` conflict.

This is the same consolidation described in MEMORY
`project_outer_scoped_join_two_rowset_distinct_base.md` ("ALL binding-keyed OUTER keys
use identity+pseudonym; merge-node + FULL-key dedup give N-way coalesce") — correct for
genuinely-FULL joins, but it swallows directional LEFT semantics once ≥2 optional
sources share one anchor.

## Why the agent churned (not a crash)

The wrong plan **executes cleanly**, so every reformulation "worked" but gave a
different (still wrong) row count: 386,671 (chained/star FULL) → later restructurings.
With no error to anchor on, the agent cycled through chained `a=b=c`, nested
`store_nr.web_nr.*`, an inner-join `other_nr` rowset, and finally a `union(...) ->
(yr,itm,cust,...)` + re-aggregate + 2-way left join (207 rows). The 2-way final form
avoids the bug but the agent never validated correctness against an anchor, and the
result still failed the reference check.

Canonical `tests/modeling/tpc_ds_duckdb/query78.preql` sidesteps all of this: it uses
the single `all_sales` model with channel-filtered conditional aggregates
(`sum(metric ? sales.channel = 'STORE')`) keyed by (year, item, billing_customer) — no
scoped join at all. It compiles to `INNER×3 + 1 LEFT OUTER JOIN` (verified via
`generate_sql`, read-only). Note: `all_sales.preql` **is present** in the run
workspace (`workspace/raw/all_sales.preql`); the agent never discovered it and went
straight to per-channel raw imports.

## Recommended action

1. **Engine fix (primary).** In `resolve_join_order_v2`, when one source is complete
   (non-partial) and ≥2 others are partial against *its* keys, anchor the join order on
   the complete source so each optional source becomes a directional LEFT_OUTER rather
   than collapsing to FULL. Equivalently, carry the scoped LEFT direction explicitly
   (an anchor/preserved-side marker) instead of inferring it from symmetric partial
   flags, so `reduce_join_types` cannot upgrade two co-anchored LEFTs to FULL.
   Target: `trilogy/core/processing/join_resolution.py` (`_score_join_candidate`
   anchor preference + `get_join_type`/`reduce_join_types`).

2. **Guidance (secondary).** Prefer the single-model conditional-aggregate idiom
   (`all_sales` + `sum(x ? channel = 'STORE')`) for cross-channel "never returned"
   questions; it avoids multi-rowset scoped joins entirely. Worth a syntax example,
   since q97 (same cross-channel family) churns the same way.

## Secondary friction (not the root cause)

The `union(...) -> (yr, itm, cust, ...)` output-tuple renaming did not collapse the
arm-qualified paths: `other_nr.the_year` raised *"Ambiguous reference ... matches
['other_nr.catalog_nr.the_year', 'other_nr.web_nr.the_year']"* and the workaround
`store_nr.web_nr.the_year` raised *Undefined concept* (run idx 54, 60). This is the
union-output-shorthand resolution surface (cf. `project_union_output_rename_drops_dupes`
/ `project_rowset_output_shorthand_resolution`) and added iterations on top of the join
bug, but is a separate, lower-severity issue.
