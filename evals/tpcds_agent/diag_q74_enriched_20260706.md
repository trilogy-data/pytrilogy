# Diagnosis: q74 FAIL in enriched leg (run 20260706-135542_enriched)

## Verdict

**AGENT error** (with proof). The agent computed exactly the right 92 customers but emitted
**8 output columns instead of the 3 the question asked for**. The scorer's multiset comparison
(`evals/common/scoring.py:_multiset`, ~line 338) hashes each row as the sorted tuple of **all**
cells — column *order* is forgiven, extra columns are not — so every one of the 92 rows
mismatched despite the underlying answer being correct.

## Symptom

- `score_query(..., 74, custom_refs_dir=tests\modeling\tpc_ds_duckdb)` reproduces:
  `status='fail', ref_rows=92, cand_rows=92, detail='result set differs from reference'`.
- Candidate columns: `['store_customer_id', 'customer_code', 'store_customer_first_name',
  'store_customer_last_name', 'store_net_2001', 'store_net_2002', 'web_net_2001', 'web_net_2002']`
  — reference (`query74.sql`) emits 3: `(customer_id, customer_first_name, customer_last_name)`.
- **Projection proof:** projecting the candidate rows to `(customer_code, first_name, last_name)`
  and comparing via the scorer's own `_multiset` gives **exact equality** with the reference
  (0 rows only-in-candidate, 0 only-in-reference). The customer set, names, ordering domain,
  ratio filter — all semantically correct. The 5 surplus columns (surrogate `store.customer.id`
  plus the four channel-year totals) are the *sole* cause of the fail.

## Task-wording quote

> "Output the customer code (the stable business identifier), first name, and last name for the
> qualifying customers." — `task.q74.txt` line 19.

The output spec is explicit and 3 columns wide. Note the agent got the q60-style identifier
choice **right** (used `store.customer.text_id as customer_code`, the TEXT business id, not the
surrogate) — but then *also* selected the surrogate id and the four intermediate totals.

The enriched prompt itself documents the spec-compliant alternative: intermediate metrics can be
kept usable in `having` while "hidden via `--`" (nested-aggregate-group-average syntax card,
~line 860 of the transcript preamble). Nothing forced the extra columns into the visible select.

## Error inventory (transcript `agent_log.q74.conversation.txt`)

Exactly one error in the whole session (line 1895-1902):

```
Resolution error in query74.preql: Discovery error: cannot merge all concepts into one
connected query (statement at line 13). The requested concepts split into 2 disconnected
subgraphs: {customer_code, store_net_2001, store_net_2002, store.customer.first_name,
store.customer.id, store.customer.last_name}; {web_net_2001, web_net_2002}.
Are you missing a join or merge statement to relate them?
```

- Trigger: the agent's first draft selected store-side and web-side aggregates with **no join**
  between the two independently imported fact models (`import raw.store_sales as store` /
  `import raw.web_sales as web`; `store.customer.*` and `web.billing_customer.*` are distinct,
  unmerged concept families).
- Should it work? **No — this is intended framework behavior**, the by-design guardrail
  requiring an explicit relation between disconnected fact models (same design as the q02/q64
  disconnect guards). The message correctly diagnoses the split and names the fix; the agent
  recovered in a single step with `union join store.customer.id = web.billing_customer.id`,
  which planned and ran cleanly. Not a framework bug, and not the cause of the score failure.

## Canonical reference check

`tests\modeling\tpc_ds_duckdb\query74.preql` (built via `generate_sql`, executed against the
sf1 DB copy with tables exposed under the `memory` catalog its datasources address):
**92 rows, 3 cols, multiset-equal to `query74.sql`.** Reference and canonical model are healthy.

(The canonical .preql cannot run through the scoring engine as-is only because its datasources
are bound to `"memory"."<table>"` — the test-harness attachment name — an environment artifact,
not a defect.)

## Classification

- **AGENT error** — output-spec violation: 5 surplus columns in the final select despite an
  explicit 3-column ask and documented `--` hidden-output support. Proof: 3-column projection of
  the candidate is multiset-identical to the reference.
- Framework: no defect. The one "cannot merge" was correct, well-worded guidance on genuinely
  disconnected concepts, recovered from immediately. `union join` + filtered aggregates +
  decimal ratio comparison all produced semantically exact results.
- Model / question: no defect (reference verified; question wording unambiguous about output).

## Suggested follow-ups (not applied)

- Grading-guidance nudge: enriched prompt could state "emit exactly the requested columns;
  hide helper metrics with `--`" — cheap insurance against this failure class.
- The passing raw-SQL leg (`results\20260706-135542_sql_bare\workspace\query74.sql`) emitted
  exactly 3 columns, confirming the delta is authoring discipline, not question difficulty.
