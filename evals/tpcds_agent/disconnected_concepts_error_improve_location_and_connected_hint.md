# Improve the "disconnected subgraphs" resolution error: add statement location + suggest connected nested equivalents

**Status:** DONE (2026-06-26) — both improvements landed; behavior unchanged, message only.
See "Resolution" at the bottom.

**Status (orig):** OPEN — error-quality improvement (the underlying behavior is correct).
**Surfaced by:** TPC-DS q75 enriched eval — the agent hit this error **9 times** and never figured
out the real cause; it blamed rowsets repeatedly instead of seeing the actual problem (separate
imports of models already reachable via `all_sales`).
**Severity:** Medium-High (agent-efficiency). This is the single biggest remaining thrash source in
the top-10; a better message would likely cut it from ~9 iterations to ~1.

## Current message

```
Resolution error in query75.preql: Discovery error: cannot merge all concepts into one connected
query. The requested concepts split into 3 disconnected subgraphs: {date.year}; {item.category,
local._yr2001_brand_id, ...}; {local._yr2001_amt, local._yr2001_qty}. Are you missing a join or
merge statement to relate them?
```

The query that produced it imported `item` and `date` **separately** alongside `all_sales`, then
used top-level `item.category` / `date.year` (disconnected copies) while the measures came from
`all_sales`. The fix is to drop the redundant imports and use `all_sales.item.category` /
`all_sales.date.year`. The current "are you missing a join or merge?" hint pointed the agent the
**wrong way** (toward merges), and gave no location.

## Improvement 1 — include the failing statement's location

Prefix the error with the line number and the start of the failing statement, like syntax errors
already do (`--> N:C` + a source snippet). Discovery runs per-statement, so the `SelectStatement`'s
`metadata.line_number` (and a one-line excerpt of the statement) is available at the raise site /
the `Resolution error in <file>:` wrapper. With multi-statement files (q75 has several rowsets +
a final select), naming *which* statement failed is essential.

## Improvement 2 — suggest connected nested equivalents (the high-leverage one)

For each concept in a disconnected subgraph, check whether the **environment** contains a concept
with the **same leaf path** under a different namespace that **is** connected to the largest
subgraph — and surface it as a "did you mean" suggestion:

```
... split into 3 disconnected subgraphs: {date.year}; {item.category, ...}; {<measures>}.
  - `date.year` is disconnected — did you mean `all_sales.date.year`? (reachable via your
    `all_sales` import and connected to the other concepts)
  - `item.category` is disconnected — did you mean `all_sales.item.category`?
These look like separately-imported copies of models already reachable through `all_sales`; drop
the redundant `import raw.item` / `import raw.date` and chain through `all_sales`.
```

This directly addresses the **separate-import mistake** — the dominant disconnection cause for
agents. The connected alternative (`all_sales.item.category`) usually is *not* among the requested
concepts, so the check must scan `environment.concepts` for addresses sharing the disconnected
concept's leaf (e.g. ends with `item.category`, or shares the final `.category` property under a
chainable path), then verify that swapping it in would join to the largest subgraph (it shares a
base/source with the connected concepts).

## Fix area

`format_disconnected_subgraphs_error` (`trilogy/core/processing/discovery_utility.py:755`) builds
the message from `subgraphs: List[List[BuildConcept]]`. It has the `environment` available at the
call sites (`raise_if_disconnected` etc.). For improvement 2, for each subgraph concept, query the
environment for same-leaf concepts in other (connected) subgraphs / reachable namespaces and emit
the suggestion; for improvement 1, thread the failing statement's `metadata.line_number` + a short
source excerpt into the message (or into the `Resolution error in <file>:` wrapper that prefixes
it). The "are you missing a join or merge?" line should stay as a fallback only when no connected
equivalent is found.

## Note

This is NOT a planner bug — q75's query is genuinely disconnected and the framework is right to
reject it. It's purely that the message doesn't point the agent at the real fix (chain through the
existing import) and lacks a location. Both are mechanical message improvements.

## Resolution (2026-06-26)

Both improvements landed in `format_disconnected_subgraphs_error`
(`trilogy/core/processing/discovery_utility.py`). Message behavior is unchanged when no environment
is threaded or no connected equivalent exists (the old "join or merge" line stays as a fallback).

**Improvement 2 (connected nested equivalents):** new `connected_equivalent_suggestions` helper.
Extracted the connectivity-component computation into `_component_map` (shared with
`disconnected_components` so reachability is judged identically). Picks the largest subgraph as the
steer-toward target, scans `environment.concepts` for a concept whose path ends with a disconnected
concept's full stripped path under an extra (chainable) namespace prefix AND is in the target's
connected component (e.g. `date.year` -> `all_sales.date.year`), preferring the shortest such
prefix. When matches exist the message lists per-concept "did you mean `<connected>`?" lines and
replaces the join/merge hint with "chain through that import" guidance. `environment` (+ graph +
`island_rowsets`) threaded into the formatter at all three raise sites (`raise_if_disconnected`,
v3 `get_query_node` dead-end, `get_priority_concept`).

**Improvement 1 (statement location):** the formatter takes an optional `line_number` and renders
`...one connected query (statement at line N).`. v4 pre-gate passes
`build_statement.meta.line_number` through `raise_if_disconnected_for`; v3 raises from deep in
discovery without it, so `get_query_node` wraps the v3 discovery call and injects the line via
`_with_line_location`. Required fixing `SelectStatement.as_lineage` (`core/statements/author.py`),
which had dropped `meta` (so `BuildSelectLineage.meta.line_number` was always None).

**Tests:** `tests/core/processing/test_disconnected_components_e2e.py` —
`test_message_suggests_connected_nested_equivalent` (faithful separate-import repro via tmp_path
imports) and `test_message_includes_failing_statement_line`. Existing message/fallback tests
unchanged and still green. Full `ruff`/`mypy`/`black` clean.
