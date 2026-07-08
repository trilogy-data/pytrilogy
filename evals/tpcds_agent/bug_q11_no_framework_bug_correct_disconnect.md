---
name: bug_q11_no_framework_bug_correct_disconnect
description: q11 626k token sink — NO framework bug. Final query is semantically CORRECT (90 rows == reference on all 4 business cols); eval "fail" is agent emitting 2 extra output columns. The Discovery/Resolution error is a CORRECT disconnect (rowset opacity + WHERE-is-filter-not-join), not a false-disconnect. Sink = cumulative context + 5 recoverable, correctly-loud errors from agent-side mistakes.
metadata:
  type: bug-triage
  query: q11
  run: 20260708-135136_enriched
  trilogy: 0.3.290
  verdict: NO-FRAMEWORK-BUG (agent + minor guidance)
---

# q11 token sink (626,650 tok, status=fail) — verdict: NO framework bug

## Symptom
- `per_query_metrics` q11: 22 iterations, 24 tool calls, **5 tool_errors**, 616,553 prompt tok / 10,097 completion = 626,650 total.
- report.md: `| 11 | fail | 90 | 90 | ... | result set differs from reference |`
- Conversation shows two flagged signatures: 4x `Syntax error … undefined concept references` (ORDER BY) + 1x `date.year` undefined + **1x `Resolution error … Discovery error: cannot merge all concepts … 2 disconnected subgraphs`** (message 31).

## Ground truth (current engine, Trilogy 0.3.290)
- Reference SQL `tests/modeling/tpc_ds_duckdb/query11.sql` → **90 rows** on the workspace DB.
- Agent's FINAL `workspace/query11.preql` builds + executes → **90 rows**, and on the 4 business columns
  `(cust_id, first_name, last_name, preferred_cust_flag)` the set diff vs reference is **0 / 0** (exact match).
- So the agent's final answer is **semantically correct**. The C1 naming swap worked: it used
  `store.customer.id` / `web.billing_customer.id` (business key) and `combined.cust_id` reports the
  business customer code, matching `c_customer_id` in the reference. No mis-pick.

## Why it scored "fail"
Not a framework defect and not a wrong result: the candidate emits **6 output columns** (adds
`web_rev_growth`, `store_rev_growth`) while the reference has **4**. The scorer flags
"result set differs" on width. Pure agent choice — the prompt asked for exactly 4 report columns.

## The Discovery/Resolution error = CORRECT disconnect (reproduced, deterministic)
Message-31 query (minimized, repro'd via `generate_sql`): the final `select` references
`combined.*` (a rowset derived from `store_rev`+`web_rev` via `subset join`) **together with**
`store_rev.first_name/last_name/preferred_cust_flag`, relating them only through a WHERE predicate
`combined.cust_id = store_rev.cust_id`.

```
ERROR: DisconnectedConceptsException
Discovery error: cannot merge all concepts into one connected query (statement at line 33).
… 2 disconnected subgraphs:
  {combined.cust_id, combined.store_2001, combined.store_2002, combined.web_2001, combined.web_2002};
  {store_rev.cust_id, store_rev.first_name, store_rev.last_name, store_rev.preferred_cust_flag}.
```

This is **intended** semantics, not a false-disconnect:
- In Trilogy, `WHERE combined.cust_id = store_rev.cust_id` is a **filter, not a join** — it creates no connectivity edge.
- `combined.cust_id` is a `coalesce(...)`-derived concept *inside* the rowset; it is a distinct
  address from `store_rev.cust_id`. A derived key contributes no pseudonym / connectivity bridge
  (same principle as `memory/project_q02_derived_join_key_no_connectivity_edge.md`).
- With no `merge`/scoped-join relating the parent rowset back to the derived rowset, the two are genuinely
  two independent sources → the engine correctly refuses to fan them together and emits a loud, actionable
  error ("Are you missing a join or merge statement to relate them?").

Raised from `trilogy/core/processing/discovery_utility.py` / surfaced via
`trilogy/core/query_processor.py` (DisconnectedConceptsException, `trilogy/core/exceptions.py`).

### Trigger matrix
| variant | result |
|---|---|
| final select refs `store_rev.*` + `combined.*`, related only by WHERE `=` (msg 31) | **DisconnectedConceptsException** (correct) |
| carry customer fields INTO `combined` via `max(store_rev.*)`, select only `combined.*` (agent's final) | builds, **90 rows, correct** |
| reference SQL | 90 rows |

The disconnect steered the agent to the correct construct (carry fields inside the rowset). Correct disconnect.

## Where the tokens actually went
- 616k prompt tok is **cumulative context re-send** across 22 turns, not a single sink. Message 3 alone is
  ~900 lines of docs/schema exploration; each subsequent turn resends the growing history.
- The 5 errors added ~10 extra turns (roughly doubling a clean run). Every one was **loud + recoverable
  with correct suggestions**:
  - msg 15 `date.year` undefined (ambiguous across imports) — correct, suggestions given.
  - msg 19/23/27 ORDER BY `first_name`/`last_name`/`preferred_cust_flag` unqualified/unaliased — correct
    (those SELECT items had no `as`, so their output address is the qualified path; bare name is undefined).
    Note the aliased item `billing_customer_code` in ORDER BY resolved fine — so ORDER-BY-by-alias works;
    the agent simply left three items unqualified across three rewrites (agent thrash).
  - msg 31 Discovery/disconnect — correct (above).

## Classification
- **Framework bug: NONE.** No false error, no silent wrong result, no bad SQL. Final answer is correct.
- **Guidance (minor):** the ORDER-BY-unqualified friction and the "WHERE `=` is not a join / re-joining a
  parent rowset needs merge or carried fields" pattern are recurring agent traps; an agent-facing hint could
  cut iterations. Not an engine defect.
- **Agent:** (a) left 3 ORDER BY items unqualified for 3 rounds; (b) tried to re-join the parent rowset via
  WHERE; (c) emitted 2 extra output columns → the actual scoring "fail".

## Proof artifacts
- Repro of the correct disconnect: minimized msg-31 preql → `DisconnectedConceptsException` deterministically.
- Final workspace query → 90 rows, exact match to reference on the 4 business columns; differs only by 2 extra columns.
