# q05 token sink — union arm joining on FULL composite grain loses a canonical key from the source map

**Run:** `evals/tpcds_agent/results/20260720-140600` (INGEST leg)
**q05 metrics:** 3,064,893 tokens (2nd-worst sink; prior baseline 1,289,056 → **>2x**), 44 iterations,
31 `trilogy run` attempts, 10 tool errors, 795s, **exit 0 / PASS**. Silent friction: the engine
eventually accepts a workaround, but rejects the natural form with an opaque internal error.

## Symptom (agent-visible)

The agent tried to bridge `web_returns → web_sales` to recover `web_site` for the returns arm of a
channel-union rollup, joining on the full web grain (`item.item_sk` + `order_number`) with two
`subset join`s inside a `union(...)` arm. Every such attempt died with:

```
Unexpected error in answer_XXXX.preql: Missing ws.order_number in
{'ws.item.item_sk': [...], 'ws.web_site.site_id': [...], 'wr.order_number': ['puffy'],
 'local.site_id_concat': []}, source map dict_keys([...])
```

An internal "Missing `<col>` in source map" from the engine's own generated pipeline = framework bug
(the planner lost a column it references). The message is un-actionable, so the agent thrashed
(44 turns) until it stumbled onto a `rowset` pre-join workaround.

## Minimal repro (against the run's `workspace/`)

```trilogy
import raw.web_sales as ws;
import raw.web_returns as wr;

with combined as union(
    (where ws.sold_date.date between '2000-08-23'::date and '2000-09-06'::date
     select concat('web_site', ws.web_site.site_id), coalesce(sum(ws.ext_sales_price),0), 0::numeric),
    (where wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date
     select concat('web_site', ws.web_site.site_id), 0::numeric, coalesce(sum(wr.return_amt),0)
     subset join wr.item.item_sk = ws.item.item_sk     -- BOTH grain keys ...
     subset join wr.order_number = ws.order_number)    -- ... = full composite grain
) -> (eid, sales, ret);
select combined.eid, combined.sales, combined.ret;
```
→ `ValueError: Missing ws.order_number in {...}` at `query_processor.py:281` / `:523`.

## Trigger matrix

| # | Case | Result |
|---|------|--------|
| A | Same arm **standalone** (not in a union), both subset joins | **OK** |
| B | 2-arm union, returns arm joins ws on **both** grain keys, ws needed for `site_id` | **FAIL** (validator raises; SQL is actually valid with validation off — false-positive) |
| F1 | union arm, **single** subset join on `item.item_sk` only | OK |
| F2 | union arm, **single** subset join on `order_number` only | OK |
| F3 | union arm, both joins (order reversed) | FAIL |
| F5 | union arm, both joins, group by `wr.order_number` (ws only in the join) | FAIL — **genuinely broken SQL**: `cast(INVALID_REFERENCE_BUG<Missing source reference to ws.order_number> as string)` |
| D | union arm, no ws join at all (group by wr key) | OK |

Trigger = **union arm + two `subset join`s that together cover the right side's full composite grain.**
Single join → fine; standalone (non-union) → fine. Whether ws is otherwise needed only changes
false-positive-reject (B) vs truly-unsourced SQL (F5); both are framework failures.

With `validate_missing` off, case B renders **valid, executable SQL (30 rows, 0 sentinels)** — so the
validator over-rejects a correct plan. F5 renders a `INVALID_REFERENCE_BUG` sentinel — genuinely lost column.

## Root cause

1. `order_number` and `item.item_sk` are both **non-nullable, non-partial `key`s** in web_sales and
   web_returns. `subset join` on two such keys lowers to **`JoinType.INNER`** —
   `trilogy/core/processing/join_resolution.py:142-155` (`get_join_type`: neither side partial,
   neither nullable → INNER).
2. Joining on the **full composite grain** makes ws/wr grain-equivalent; the arm's group/merge node
   canonicalizes the key to a **pseudonym address `ws.order_number`** in its `output_concepts`, while
   the `source_map` keys it only under the anchor `wr.order_number` (confirmed by instrumenting
   `resolve_concept_map`: final node emits `wr.order_number` in the map, `ws.order_number` as output).
3. The node that would reconcile this — the key-equivalence `source_map` repair in
   **`trilogy/core/processing/nodes/merge_node.py:689-721`** — only collects `outer_pairs` from
   **OUTER joins** (`LEFT_OUTER/RIGHT_OUTER/FULL`, line 693-694) and only backfills when
   `len(combined) > 1` (guard at line 718). Because this join is **INNER**, its pairs never enter
   `outer_pairs`; `_key_equivalence_classes` never builds a class for `order_number` (verified: no
   class ever logged), so the pseudonym output key `ws.order_number` is **never given a source_map
   entry**.
4. Downstream, the strict source-map validators reject the plan:
   `trilogy/core/query_processor.py:276-282` (`generate_source_map`) and
   `:506-525` (`datasource_to_cte`, the `CONFIG.validate_missing` block that raises the exact
   agent-seen message). When validation is off, the renderer's pseudonym fallback saves case B but not
   F5, where `trilogy/dialect/base.py` (~1482/1632, `Missing source reference to {c.address}`) stamps
   the `INVALID_REFERENCE_BUG` sentinel.

Net: **INNER-merged, full-composite-grain key equivalences inside a union arm are outside the coverage
of the merge-node source-map repair (OUTER-only), leaving a canonical pseudonym key unsourced.**

## Why INGEST-leg-specific

Canonical `tests/modeling/tpc_ds_duckdb/query05.preql` uses the consolidated `all_sales` model (returns
folded into sales, single grain) and never cross-joins returns→sales, so it dodges the construct.
The auto-ingested `raw/` model has no such bridge, so recovering `web_site` for web returns *forces*
the returns-arm-joins-sales-on-full-grain pattern that trips the bug.

## Classification

**REAL FRAMEWORK BUG** (loud but opaque internal error → un-actionable → token sink). Not agent, not
guidance/idiom: the rejected query is a legitimate Trilogy construct and (case B) produces correct SQL
once the over-strict validator is bypassed.

## Pass genuineness

Genuine. Final `workspace/query05.preql` uses a `rowset` pre-join workaround (`web_ret_site <- ... subset
join`) and scores **100 ref rows vs 100 cand rows, status pass** (`scoring.score_query`, refs =
`tests/modeling/tpc_ds_duckdb`). The pass is a workaround, not eval luck.

## Suggested fix locus (do NOT fix here)

Extend the `merge_node.py:689-721` key-equivalence source-map repair to also cover INNER-merged key
classes (seed `outer_pairs` from all join `concept_pairs` with distinct left/right addresses, or drop
the OUTER-only filter), and/or drop the `len(combined) <= 1` guard so a lone-partner source still
backfills the pseudonym member — so a union arm joining on the full composite grain keeps every
canonical grain key in the source map.

## RESOLUTION 2026-07-20 — FIXED, but the root cause was NOT the source-map repair

Confirmed REAL, but two of this report's claims were disproven during the fix:

- The suggested merge-node source-map repair was implemented and then **removed**: it silenced the
  error but unmasked a **wrong-results plan** (returns fanned out ~15-20x — the enrichment side had
  been grouped to item grain and the join paired `item` only). The strict validator was correctly
  rejecting a plan that had already lost the key.
- The "validator over-rejects a correct plan" claim (case B, validation off, 30 rows / 0 sentinels)
  is wrong: those 30 rows silently carried **zero** for every `ret` value — a *different* bug (cross-arm
  anonymous-output address collision) had already pruned the returns aggregate out of the plan.

Actual defects fixed (each value-verified against raw-SQL references on the run's `tpcds.duckdb`):

1. **Anonymous select outputs skipped per-rowset mangling** (`parsing/v2/rules/select_statement_rules.py`).
   Identical unaliased expressions in different union arms (`0::numeric` in each) shared one address,
   collapsing the align items' positional identity: `find_source` resolved the wrong arm's column and
   the real aggregate was dead-pruned — silent zeros.
2. **Subgraph decomposition dropped a side's member of a merged key** (`node_merge_node.py::extract_ds_components`).
   The steiner tree keeps only the cheapest path to the canonical, so the side reaching it through the
   pseudonym/scoped-join edge never materialized its own column; the equality dropped out of the merge
   join (source-map strand in case B, silent fan-out in case F5). Components now attach each target
   key's member address wherever a component datasource binds it directly or as an AUTHORED scoped-join
   mate. Authored pairs ONLY: matching through the base graph's pseudonym edges also links role-aliased
   imports of one model (`pos_address` vs `current_address`), which cross-pollutes components until
   unrelated dimensions fuse into a 1=1-joined subgraph — first cut hung q64 execution that way.
3. **Join-key render pinned to the FROM base a key provided by a rendered join partner**
   (`dialect/common.py::_render_left_concept`), emitting a column the base CTE doesn't have
   (`Binder Error: ... does not have a column named ws_item`). The stale-reference redirect now skips
   nodes that render in the consumer's own FROM/JOIN chain.
4. **`find_source` had no recovery for scoped-join-canonicalized arm outputs** (`models/build.py`):
   an arm selecting `wr.order_number` whose CTE emits the canonicalized `ws.order_number` raised
   `UnionOutputResolutionError`. It now accepts a scoped-join partner / pseudonym twin among the CTE
   outputs.

Coverage: `tests/test_union_arm_subset_join_full_grain.py` (case B, F3 reversed order, F5
partner-only-in-join, standalone control — all value-asserted). Cases B/F3/F5 verified row-exact
against hand-written SQL references over the run's real data.
