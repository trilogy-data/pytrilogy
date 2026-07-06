# q11 "cannot merge" token sink (224k → 624k) — 2026-07-06

**Run:** `evals/tpcds_agent/results/20260706-222300` — q11 **PASSES** (exit 0, 130s)
but burned **623,692 tokens** / 19 iterations / 22 tool calls / 3 tool errors
(vs 224k in `20260706-135542_enriched`). Error seen twice:
`Resolution error in query11.preql: Discovery error: cannot merge all concepts into
one connected query ... 2 disconnected subgraphs: {store_rev_2001, store_rev_2002};
{web_rev_2001, web_rev_2002, ws.billing_customer.id}`.

## VERDICT: NOT a framework bug. Correct disconnect + guidance gap (agent model-choice).

The "cannot merge" is **CORRECT**, not a false-disconnect. Reproduced deterministically
(build-only, no DB) against the run's own workspace model.

### What the agent did wrong
q11 is a cross-channel (STORE vs WEB) per-customer year-over-year question. The workspace
ships a **curated `raw/all_sales.preql`** unified model whose header literally says
*"Use as default for multi-channel analysis (WEB / CATALOG / STORE) or any two channel
combination"*, and the peer single-channel models carry *"For cross-channel questions,
prefer `all_sales`"*. **The agent received all of these descriptions** (agent_log.q11
conversation, file-list at msgs 3–4, lines 950/984/1088) yet at msg 5 chose to
`import raw.store_sales as ss` + `import raw.web_sales as ws` — two **separately-namespaced
single-channel fact models**. Their customer concepts `ss.customer.id` and
`ws.billing_customer.id` are distinct addresses that Trilogy does **not** auto-merge
(by design — bill vs ship vs peer-model customers are intentionally distinct). So the four
`store_rev_*` / `web_rev_*` auto-measures split into two islands. The engine correctly
demanded a join/merge.

The agent's first recovery (msg 15) aliased `ss.customer.id as cust_id` **inside a rowset**
and then `union join store_cust_2002.cust_id = ws.billing_customer.id` — which bridges the
rowset *alias* but leaves the raw-keyed `store_rev_*` auto-measures stranded (a real footgun,
not a bug: aliasing in a rowset mints a new concept). It then thrashed through union-join
spellings (`not distinct`×8, parse errors×5) before landing on the passing chained-`union join`
form in `workspace/query11.preql`. That thrash is the 400k burn.

### Reproduction matrix (build-only, `Environment(working_path=<run workspace>)`)
| # | Spelling | Result |
|---|----------|--------|
| A | 2 raw models, 4 auto-measures, no bridge | **DisconnectedConceptsException** (correct) |
| A' | 2 raw models, `select ss.customer.id, ws.billing_customer.id` (bare dims) | **still disconnects** — proves the two customer concepts genuinely don't auto-merge |
| B | canonical `import all_sales as sales` (full query11.preql) | **compiles OK** |
| C | 2 raw models + `merge ss.customer.* into ~ws.billing_customer.*` | **compiles OK** |

A' is the smoking gun: selecting only the two customer keys already disconnects, so the
model choice — not any measure/rowset detail — is the root cause. The intended path (B)
resolves cleanly.

## Root cause of the *token sink* (not a defect, a guidance hole)
The disconnect error's own suggestion mechanism,
`connected_equivalent_suggestions` (`trilogy/core/processing/discovery_utility.py:762`,
consumed by `format_disconnected_subgraphs_error` :817), only fires for the
**suffix-twin re-import** mistake — a stranded concept whose address is a suffix of a
connected concept (e.g. `date.year` vs `all_sales.date.year`). Here neither
`ss.customer.id` nor `ws.billing_customer.id` is a suffix of the other, so **no suggestion
fired** and the agent got only the generic tail:
`Are you missing a join or merge statement to relate them?` (:861). It never named
`all_sales`, even though a curated unified model exposing that exact customer dimension
existed in the environment.

## Improvement that would have saved ~400k tokens (do NOT fix here)
1. **Error-message steering (engine):** when disconnected fact subgraphs each reach a
   *shared dimension* (customer) that a single curated model in the environment exposes on
   one grain, name that model in the hint — e.g. *"`store_rev_*` and `web_rev_*` are on
   separate single-channel facts; `all_sales` unifies them — import `all_sales` and filter
   by `channel`."* Extend `connected_equivalent_suggestions` beyond suffix-twins to a
   "shared-dimension curated-union" detector. This is the highest-leverage fix.
2. **Agent guidance (cheaper):** the file-list `prefer all_sales` hints are advisory and were
   ignored. For any question naming ≥2 channels, the prompt should hard-steer to `all_sales`
   before importing peer `*_sales` models.

## Notes
- `workspace/raw/repro.preql` is a stray leftover artifact from another task (an all_sales
  net-profit/return snippet); unrelated to q11.
- Guards already exist for the suffix-twin case; the shared-dimension-union case has none.
- The passing final query is correct output-wise (it scores pass); the concern is purely the
  624k-token path to get there.
