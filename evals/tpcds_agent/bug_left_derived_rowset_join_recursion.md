# BUG: LEFT scoped join on a derived-expression key between two rowsets recurses forever

**Classification:** framework bug — uncaught `RecursionError`. Surfaced to the CLI/agent as the
opaque `Unexpected error: ... maximum recursion depth exceeded`, with no line/construct hint, so
an agent cannot self-correct and just retries (observed driving a single query to 2.3M prompt
tokens / 50 iterations against the iteration cap).

**Likely a regression from the in-flight join spike.** The recursion cycle runs through
`_enrich_via_derived_join_key` in `node_generators/rowset_node.py`, which **does not exist in
HEAD** (`git show HEAD:... | grep -c` = 0) — it is uncommitted code from the derived-rowset-join
fix. Whoever owns that spike should own this. Verify against HEAD before assuming pre-existing.

**Status:** FIXED 2026-06-30. `_producible_derived_join_keys` now declines any
pairing whose other-side key is producible from THIS rowset (the LEFT-anchor
substitution collapses `fut.period` onto the anchor's derived key, so its inputs
are `agg`'s own outputs — sourcing it re-entered `agg`). LEFT now falls through to
the standard left-anchor path and resolves as a real `LEFT OUTER JOIN` (correct
rows; unmatched anchor row kept, ratio NULL) — better than the clean-error floor.
INNER unchanged. Plus defense-in-depth: a bare `RecursionError` is now labelled a
clean "Resolution error: query could not be planned; this is a bug"
(`scripts/common.py`) — a RecursionError is always a framework bug — and bucketed
`planner-recursion` in eval analysis (`evals/common/analyze_run.py`).
Tests: `tests/test_left_derived_rowset_join.py`,
`tests/scripts/test_config.py::test_handle_execution_exception_labels_recursion_errors`.

---

## What it is (generic)

A scoped **`left join`** between two rowsets whose key is a **derived expression**
(`a.k + N = b.k`, any non-identity expression), where the final select **re-aggregates** the
rowset measures, never terminates. The analogous **`inner join`** resolves fine, and a
**plain-equality** `left join` (`a.k = b.k`) resolves fine — so the bug is the *intersection* of
LEFT-anchor + derived-expression key. It is NOT specific to any one query, to unions, to a
filter rowset, or to the number of rowsets; those just made the discovered instance bigger.

## Minimal generic repro

Self-contained (no data; `generate_sql` alone triggers it). Run:

```
.venv/Scripts/python.exe evals/tpcds_agent/repro_left_derived_rowset_join_recursion.py
```

(committed alongside this file). Core:

```trilogy
import sales as s;                                   -- key sid; property period, amt
rowset agg <- select s.period, sum(s.amt) as tot;
rowset fut <- select s.period, sum(s.amt) as tot;

select agg.period, sum(agg.tot) / sum(fut.tot) as r
left join agg.period + 53 = fut.period;              -- RECURSION
```

## Trigger matrix

| join type | key | re-aggregated? | result |
|---|---|---|---|
| `left`  | derived (`+ 53`) | yes | **RecursionError** |
| `inner` | derived (`+ 53`) | yes | OK (resolves) |
| `left`  | plain equality   | yes | OK |
| `left`  | derived (`+ 53`) | no (plain projection) | OK (offset-contract path) |

Both LEFT *and* the derived key are necessary; filtered vs plain aggregate does not matter
(both recurse). Same axes as the sibling fix in
`evals/tpcds_agent/bug_derived_rowset_join_key_reaggregate_disconnect.md` — that one fixed the
INNER/disconnect case; LEFT was left recursing.

## Root cause / recursion cycle

`_enrich_via_derived_join_key` (`rowset_node.py:463`) materializes the derived key locally then
calls `source_concepts(...)` for the still-missing optionals **plus the other side's key**, so
the merge has a real column. Its docstring asserts "the other rowset is never re-sourced through
this one (which would recurse)" — that invariant **holds for `scoped_inner_join_keys` but breaks
for `scoped_left_anchor_keys`**: sourcing the other side's key (a BASIC/derived concept) routes
back into `_generate_rowset_node` for the *same* rowset, which re-enters `_enrich_rowset_node` →
`_enrich_via_derived_join_key` with no `history`/visited guard to cut the loop.

Observed cycle (from the minimal repro):

```
search_concepts → _search_concepts → generate_node
  → _generate_rowset_node → gen_rowset_node → _enrich_rowset_node
  → _enrich_via_derived_join_key            (rowset_node.py:463)
  → source_concepts → ... → _generate_basic_node → gen_basic_node   (sourcing fut.period)
  → search_concepts → _generate_rowset_node → gen_rowset_node       (re-enters agg's rowset)
  → _enrich_rowset_node → _enrich_via_derived_join_key → ...          (loops)
```

`_producible_derived_join_keys` (`rowset_node.py:107`) includes `scoped_left_anchor_keys` in the
registry it scans, so the LEFT derived key enters the same enrichment path the spike built for
INNER — but the left-anchor topology re-sources the other rowset rather than relating to it by
pseudonym.

## Fix direction (for the executor)

Two things, ideally both:

1. **Terminate the recursion.** Either (a) exclude `scoped_left_anchor_keys` from
   `_producible_derived_join_keys` until the left-anchor merge is correctly handled, or (b) make
   `_enrich_via_derived_join_key` source the other side's key WITHOUT re-entering this rowset's
   generation (thread a `history`/visited guard keyed on the rowset being enriched, mirroring how
   the inner path avoids re-sourcing). The right fix likely makes the LEFT case resolve the same
   way INNER does (C1 in the matrix), not just error out.
2. **Never surface a bare RecursionError.** Independent of the above: discovery should guard
   recursion depth / a visited set and raise a clean `UnresolvableQueryException` (or resolve)
   rather than letting a `RecursionError` escape as "Unexpected error: maximum recursion depth
   exceeded". The opaque message is what makes the agent burn the whole iteration budget. This is
   the same defect class as the q05 recursion guards (see memory `project_q05_*recursion*`).

## Guardrails (must not regress)

- `tests/test_rowset_offset_join_contract.py` — derived-key join, plain projection (must stay OK).
- The INNER derived-key + re-aggregation case (sibling bug's fix) — `repro_derived_rowset_join.py`
  R1/R5 must stay green.
- Scoped-join correctness memories: q29 (nullable inner not widened to full), q78 (left/inner
  multi-partial anchor — directly touches `scoped_left_anchor_keys`), q21.
- A genuinely unrelatable LEFT-join-across-rowsets query must raise a clean error, not recurse.

## File pointers

- `trilogy/core/processing/node_generators/rowset_node.py:463` — `_enrich_via_derived_join_key` (NEW, spike; recursion entry).
- `trilogy/core/processing/node_generators/rowset_node.py:107` — `_producible_derived_join_keys` (includes `scoped_left_anchor_keys`).
- `trilogy/core/processing/node_generators/rowset_node.py:505` — `_enrich_rowset_node` (caller).
- `trilogy/core/processing/concept_strategies_v3.py:106` — `search_concepts` (the re-entered loop; carries `history`).
- `trilogy/scripts/common.py` / `evals/analyze_run.py` — where "Unexpected error" is labeled; a RecursionError should map to a clean category.
