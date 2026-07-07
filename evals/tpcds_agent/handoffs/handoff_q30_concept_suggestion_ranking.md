# Handoff — q30: `_find_similar_concepts` ranks suggestions by dict order, omits the correct key

**Verification:** ⚠️ SUBAGENT-REPORTED — CONFIRM the repro before fixing. Detail report:
`bug_q30_avg_threshold_sink_20260706.md`.

## Confirmed-by-subagent bug (DX defect → silent wrong result)
When the agent referenced a bare-namespace concept `by web_returns.billing_customer`, the
undefined-concept error's `Suggestions:` listed only deep wrong-namespace keys
(`web_returns.web_sales.billing_customer.demographics.*`) and OMITTED the obvious correct
`web_returns.billing_customer.id`. The agent trusted the suggestion, rerouted through the ORIGINAL
order's bill-to customer (WS_BILL_CUSTOMER_SK — a semantically different customer), and produced a
silent wrong result (candidate ids overlap reference 2/93; correct path → 94/99). Also a ~590k token sink.

## Root cause (locus)
`trilogy/core/models/environment.py:390` `_find_similar_concepts`: `path_matches` (L416-425) collects
subsequence matches but iterates `self.keys()` in DICT-INSERTION order with NO relevance sort, then
hard-caps at 6 (L459-463). `web_sales` is imported before the `customer as billing_customer` alias, so
deep `web_sales.billing_customer.demographics.*` keys sit at dict index ~107 while the correct shallow
`web_returns.billing_customer.id` sits at ~554 — the 6-cap is consumed by the deep wrong matches. The
correct key exists but is never returned.

## Fix direction
Rank `path_matches` before the cap by relevance: fewest extra segments beyond the query
(`len(key_segs) - len(q_segs)`) and/or longest contiguous prefix — so `...billing_customer.id` (extra 1)
outranks `...web_sales.billing_customer.demographics.id` (extra 3). Optional: add an "an alias is a
namespace; did you mean its key `X.id`?" hint. NOTE: q11 leans on a related helper
(`discovery_utility.py:762`) that fails to name curated `all_sales` in disconnect errors — same DX theme,
consider together.

## Guard test
`tests/` unit test on `_find_similar_concepts`: given an env where a shallow correct key is inserted
AFTER many deep near-miss keys, assert the shallow key appears in the top-6 suggestions for the bare
query. General fix — improves suggestions across every query, not just q30.
