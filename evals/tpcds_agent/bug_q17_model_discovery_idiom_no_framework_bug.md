# q17 token sink (1,520,398 tokens, FAIL) — diagnosis

Run: `evals/tpcds_agent/results/20260711-042547_enriched` (enriched leg)
Verdict: **NO framework codegen bug.** Root cause = model-discovery/guidance defect
+ join-idiom misuse. Every error the agent hit was a clean agent error with an
actionable message. This is NOT the prior "authored-join discovery injection
(q17/q25 unprojected property-key drop)" class (that is landed/fixed); it is a
fresh model-discovery miss.

## Symptom
- Agent burned 1.52M tokens, produced a query that runs cleanly (exit 0) but
  returns the wrong grain: **9,886 rows over 9,384 groups (100 after LIMIT)** vs
  the reference's **23 rows**. `score_query` → fail (cand 100 / ref 23).
- Two tool errors seen: `Undefined concept: cs.item_sk` and a parse error at
  `select` — both clean agent typos (see below).

## The question maps 1:1 to an available bespoke model the agent never used
- q17 is exactly "store sales + store returns + catalog sales as a unit."
- `raw/catalog_store_returns.preql` exists in the workspace, described in the
  file-list as *"Bespoke model for analyze catalog sales, store sales, and store
  returns as a unit … used for specific analysis of patterns across these 3
  datasets."*
- The **canonical** `tests/modeling/tpc_ds_duckdb/query17.preql` imports exactly
  that model and, on the current engine, **builds and returns 23 rows** (matches
  the reference). Confirmed by rewriting its import to `raw.catalog_store_returns`
  and running via the harness → `ROWS 23`.
- The agent NEVER imported or explored `catalog_store_returns`. Its only imports
  across the whole trajectory were `raw.store_sales`, `raw.catalog_sales`,
  `raw.item`. It saw the file in the list but the `store_sales`/`catalog_sales`
  descriptions steer cross-channel work to **`all_sales`** (which the agent
  looked at and rejected), not to the correct bespoke model. Guidance defect:
  the fact-table "prefer all_sales" hints point away from the model that fits.

## Why the manual reconstruction fails (idiom gap, not engine bug)
- Reference is a 3-way INNER intersection (store_sales ⋈ store_returns ⋈
  catalog_sales on customer+item), so only 23 (item_id, item_desc, s_state)
  groups survive; catalog has no store dimension, so `s_state` comes from the
  store side.
- The agent computed three independent aggregate rowsets and combined them with
  chained **`union join`** (outer stack/coalesce). Union join keeps every group
  from every member → the store universe (9,384 groups) instead of the 23-group
  intersection, and `cs_stats` (grouped only by item_id+item_desc, no state)
  lands on its own rows / as nulls. This is union-join semantics working as
  designed; it is the wrong join type for an inner alignment. There is no clean
  subset/union-join expression of the 3-way inner intersection from raw facts —
  which is precisely what the bespoke model exists to provide.

## Errors reproduced — all clean agent errors
- `Undefined concept: cs.item_sk` (msg 52): agent wrote `cs.item_sk` in a WHERE
  after aliasing the output `... as item_sk`; suggestion list correctly offered
  `cs.item.sk`. Not a framework bug.
- Parse error `expected select_item` at `select` (msg 22): agent pasted a
  truncated/incomplete file body. Not a framework bug.
- Cross-rowset tuple membership `(cs.item.sk, cs.billing_customer.sk) in
  (ss_base.item_sk, ss_base.cust_sk)` — **works** (verified in isolation, 20
  rows). Subset join and union join both **codegen and execute** without error.

## Trigger matrix
| Form | Result | Note |
|---|---|---|
| canonical `.preql` via `catalog_store_returns` | 23 rows ✓ | correct |
| agent final (5× `union join` of 3 aggregate rowsets) | 9,886 rows ✗ | outer stack → wrong grain |
| cross-rowset tuple `in` filter (isolated) | 20 rows ✓ | construct is fine |
| plain `stddev` grouped over store_sales | runs ✓ | see nondeterminism below |

## Nondeterminism observed — real but benign, NOT a Trilogy bug
- Re-running the agent's query gives different result multisets across runs; the
  differences are **only in stddev/cv columns** (2,773 cs_stddev / 2,307 cs_cv /
  etc.). counts and averages are fully stable.
- Reproduced with a **plain** `stddev(...) group by (item.id, state)` over
  store_sales — also nondeterministic. This is DuckDB parallel `STDDEV_SAMP`
  float summation-order drift, inherent to raw SQL too; the scorer's float
  tolerance absorbs it. It is NOT what causes the scoring failure (wrong grain
  is). Flagging only so it is not mistaken for a codegen bug on future traces.

## Recommendation (no engine fix)
1. Model/guidance: make `catalog_store_returns` discoverable for this scenario —
   e.g. add a hint in the `store_sales`/`catalog_sales` descriptions ("for store
   sales cross-referenced with catalog sales/returns, see
   `catalog_store_returns`") so the "prefer all_sales" steer does not shadow it.
2. Idiom doc: chained `union join` across independent aggregate rowsets is an
   outer stack, not an inner grain-align; the 3-way inner case needs the bespoke
   model (or a single multi-measure select over it).

## Repro harness
`scratchpad/repro17.py` (score/gen/rows against the run workspace);
`scratchpad/canon17_raw.preql` (canonical with `raw.` import → 23 rows).
