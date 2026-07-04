# q64 token sink — subset/union (coalescing) join subordinate key vanishes across a rowset boundary

Run: `evals/tpcds_agent/results/20260703-151512/` — q64 burned **2,099,128 tokens**, FAILED
(reference 2 rows, candidate **12 rows** — silent fan-out, not the "2 vs 2" the harness summary implied;
`report.json` `queries[id=64]` shows `cand_rows: 12`).

## Symptom

The agent's natural, correct formulation of q64 is:
1. `subset join` store_sales ↔ store_returns on `ticket_number` **and** `item.id` (the store-return semijoin),
   aggregate per (item/store/year/…) into a rowset `base_agg`;
2. downstream, reference `base_agg.ss.item.id` to self-pair the two years.

Step 2 fails at **render** time (not resolution) with:

```
Could not render the query: Missing source reference to ss.item.id.
A planned reference has no backing source CTE -- typically an unsupported
cross-rowset or membership shape the planner could not wire.
```

The generated base CTE materializes the collapsed join key as
`coalesce("sr_item_items"."I_ITEM_SK","ss_item_items"."I_ITEM_SK") AS "sr_item_id"` —
i.e. under the **anchor** (`sr.item.id`) address only. The downstream statement asks for the
**subordinate** authored address `ss.item.id`, which no CTE column backs → sentinel.

The agent (conversation msg 73–74) read this as "`ss.item.id` is being lost in the intermediate
CTEs", dropped `item.id` from the self-pair key, and partitioned the year-pairing window on
`product_name` + store instead. Product name is NOT unique per item in TPC-DS, so cross-item pairs
leak in → **12 rows instead of 2** (the last 2 candidate rows even share the reference's address keys
but with wrong wholesale/list sums). This is the silent wrong-result that ended the run.

## Minimal repro (self-contained BODY)

```trilogy
import raw.store_sales as ss;
import raw.store_returns as sr;

with base_agg as
where ss.item.current_price between 65 and 74
select ss.item.id, count(ss.line_item) as sale_lines
subset join ss.item.id = sr.item.id
;

select base_agg.ss.item.id, base_agg.sale_lines;   -- RENDER: Missing source reference to ss.item.id
```

Change the last line to `select base_agg.sr.item.id, base_agg.sale_lines;` → **OK, 165 rows**.
The subordinate side of the join is what's lost; the anchor side is fine.

## Trigger matrix

Base = `with base_agg as … <JOIN> join ss.item.id = sr.item.id ; <downstream ref>`.

| # | Variant | Result |
|---|---------|--------|
| A | `subset` join item.id, downstream ref **`ss.item.id`** (subordinate) | **RENDER_SENTINEL** |
| B | `subset` join item.id, downstream window on `product_name`, item.id still in base select | **RENDER_SENTINEL** |
| C | `subset` join on **ticket only** (item.id NOT a join key), ref `ss.item.id` downstream | OK rows=165 |
| D | `inner` join item.id | HydrationError: inner not allowed in scoped joins (expected) |
| E | `left`  join item.id, ref `ss.item.id` downstream | **OK rows=165** |
| F | `subset` join item.id, downstream **plain** `select base_agg.ss.item.id` (no window) | **RENDER_SENTINEL** |
| single | `subset` join item.id, **single statement** selecting `ss.item.id` (no downstream) | OK rows=165 |
| noref | `subset` join item.id, downstream does NOT reference item.id | OK rows=108 |
| anchor | `subset` join item.id, downstream ref **`sr.item.id`** (anchor) | **OK rows=165** |

Join-type sweep, downstream ref of the subordinate `ss.item.id`:

| join type | downstream ref subordinate key |
|-----------|-------------------------------|
| `full`   | **RENDER_SENTINEL** |
| `left`   | OK rows=165 |
| `subset` | **RENDER_SENTINEL** |
| `union`  | **RENDER_SENTINEL** |

### Minimal pass/fail boundary
FAIL iff **all** of: (a) the join is a **coalescing** type (`full`/`subset`/`union`) — `left` is fine;
(b) `item.id` is the **join key**; (c) the join lives inside a rowset (`with … as`) whose output is
consumed by a **downstream** statement; (d) that downstream references the **subordinate** side
(`ss.item.id`), not the anchor (`sr.item.id`). Window vs plain select is irrelevant (F). Single
statement is fine.

## Root cause (file:line)

A coalescing scoped join (`full`/`subset`/`union`) collapses `ss.item.id = sr.item.id` into ONE
canonical output expression `coalesce(<anchor>, <sub>)` materialized in the rowset body CTE under the
**anchor address** (`sr.item.id`). The subordinate authored address `ss.item.id` survives only as a
pseudonym of that canonical.

`trilogy/core/processing/node_generators/rowset_node.py` `_build_translation_node` (~line 282) and
`_unhide_referenced_body_locals` (~line 259) are the machinery meant to carry a "collapsed join key
whose authored source only exists inside the body as the join canonical" across the rowset boundary
(see the docstring at rowset_node.py:291–297). That mapping exposes the **anchor** address but does
**not** register the **subordinate** authored address (`ss.item.id`) as a sourceable pseudonym of the
body's canonical column. So a downstream plan reference to `base_agg.ss.item.id` finds no backing
source map entry.

The renderer then emits the sentinel column
`INVALID_REFERENCE_STRING("Missing source reference to ss.item.id")` at
`trilogy/dialect/base.py:1407`, and strict mode raises the hard `ValueError` at
`trilogy/dialect/base.py:2656`.

## Classification & regression

**Real framework bug** (not agent error, not pure guidance). A concept that is (i) selected into a
rowset's output and (ii) a coalescing-join key cannot be referenced downstream by its authored
(subordinate) name — a legal, natural shape.

Regression assessment: the defect is present for the pre-existing `full` join too, so the underlying
boundary gap likely predates the join refactor. But it was **newly exposed** by the refactor
(`956e7303b hacky_joins`, `dcc62ed78 union_checkpoint`) introducing `subset`/`union` as the
*recommended* idiom for exactly this store_sales↔store_returns semijoin — which is why q64 tokens
swung 417K→2.1M across it. The tests/modeling canonical `query64.preql` still builds and its reference
SQL still returns the correct 2 rows; the canonical dodges the bug by chaining the base key into the
join group (`full join agg_99.item_sk_99 = agg_00.item_sk_00 = ss.item.id`) rather than referencing a
subordinate side across a boundary.

## Workarounds (for guidance, not a fix)
- ~~Reference the **anchor** side downstream (`base_agg.sr.item.id`)~~ — **NO LONGER WORKS**, see
  re-confirmation below. The only surviving workaround is to chain the base concept into the join group
  as the canonical query does, or select *both* sides and avoid a coalescing join key entirely.

---

## Re-confirmed against 20260703-212501 (2026-07-03)

**STILL REPRODUCES — core symptom unchanged; one workaround regressed.**

Target run `evals/tpcds_agent/results/20260703-212501/` q64: burned **850,511 tokens**, still FAILED
(`report.json` `cand_rows: 595` vs `ref_rows: 2` — a fan-out, not the sentinel). Working tree since the
original handoff carries a q59 union-join optimizer change (`value_set_join_upgrade.py`) and a q66
group-by fix (`execute.py`).

**Did the agent hit the sentinel this run?** No — the agent *navigated around* the failing shape. Its
`workspace/query64.preql` never references a coalescing-join subordinate key across a boundary: it
builds membership rowsets with `union join`, filters the `agg` rowset with `... in
items_with_return.ret_item_id` (membership, not a scoped join key), and self-pairs the two years on
`item_text_id + store_name + store_zip` (line 160-162), never on `item.id`. No `Missing source
reference` appears anywhere in `agent_log.q64.conversation.txt`; the only resolution errors it hit were
`Discovery error: cannot merge ... disconnected subgraphs {cs.item.id ...}` (msg 33, an unrelated
cross-model `auto` mistake it corrected). It still failed with 595 rows because the `item_text_id`/store
self-pair key over-counts — i.e. the SAME class of cross-item fan-out the original bug forces, arrived
at by a different route. So the underlying defect is still what makes q64 unwinnable via the natural
`subset`/`union` idiom; the agent just burns tokens avoiding it and lands on a fan-out anyway.

**Minimal repro on the CURRENT engine** (built against
`results/20260703-212501/workspace/`, `base_agg` selecting `ss.item.id`,
`subset join ss.item.id = sr.item.id`, downstream `select base_agg.<ref>.item.id`):

| join type | downstream ref | 2026-07-02 handoff | 2026-07-03 re-check |
|-----------|----------------|--------------------|---------------------|
| `subset` | `ss.item.id` (subordinate) | RENDER_SENTINEL | **RENDER_SENTINEL** (unchanged) |
| `full`   | `ss.item.id` | RENDER_SENTINEL | **RENDER_SENTINEL** (unchanged) |
| `union`  | `ss.item.id` | RENDER_SENTINEL | **RENDER_SENTINEL** (unchanged) |
| `left`   | `ss.item.id` | OK rows=165 | **OK rows=165** (unchanged) |
| `subset` | `sr.item.id` (anchor) | **OK rows=165** | **UndefinedConceptException** — DRIFT |
| `subset` | ticket-only join, ref `ss.item.id` | OK rows=165 | **OK rows=165** (unchanged) |

Exact current error (subordinate, `subset`/`full`/`union`), verbatim:
```
Could not render the query: Missing source reference to ss.item.id. A planned reference has no
backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could
not wire. Review the rowset/join structure (or file an issue if the query looks valid).
```

**Root cause — UNCHANGED.** Still the rowset translation-node boundary not exposing the collapsed
join-key concept's authored address downstream. `trilogy/core/processing/node_generators/rowset_node.py`
`_unhide_referenced_body_locals` (now at **L260**) / `_build_translation_node` (now at **L283**,
docstring **L294-296**). Sentinel emitted at `trilogy/dialect/base.py:1407`
(`INVALID_REFERENCE_STRING(f"Missing source reference to {rc.address}")`); strict-mode hard
`ValueError` at `trilogy/dialect/base.py:2655-2660`. All four loci match the original handoff (line
numbers are unshifted). **Recommended fix locus is still rowset_node.py `_build_translation_node` /
`_unhide_referenced_body_locals` (~L260-297).**

**What the working-tree fixes DID change (drift, not a fix):** the collapse *canonical alias* moved to
the **subordinate, selected** address. Single-statement now materializes the join key as
`coalesce("sr_item_items"."I_ITEM_SK","ss_item_items"."I_ITEM_SK","ss_store_sales"."SS_ITEM_SK") AS
"ss_item_id"` — i.e. under `ss.item.id` (the concept actually in the SELECT), whereas the original
handoff observed `... AS "sr_item_id"` (the anchor). This is consistent with the `value_set_join_upgrade.py`
q59 change reorienting which operand is canonical. Two consequences: (1) the subordinate address is now
the canonical *and still can't be sourced downstream* — the sentinel is unchanged, confirming the boundary
gap is independent of which side is canonical; (2) the anchor `base_agg.sr.item.id` is no longer an output
pseudonym, so the handoff's "reference the anchor side" workaround now raises
`UndefinedConceptException: base_agg.sr.item.id` (suggests `ss.item.id`). The optimizer change made the
anchor workaround *worse*, and fixed nothing about the actual defect. The q66 `execute.py` change is
unrelated and had no effect here.

**Canonical reference intact:** `score_query(..., custom_refs_dir=tests/modeling/tpc_ds_duckdb)` builds
`tests/modeling/tpc_ds_duckdb/query64.preql` and returns `ref_rows=2` — the canonical still dodges the
bug by chaining the base key into the join group.
