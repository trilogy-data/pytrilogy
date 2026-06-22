# Bug: outer (LEFT/FULL) scoped join between two aggregate rowsets drops the cross-rowset edge

**Status:** RESOLVED 2026-06-22. `tests/test_scoped_join_rowset_outer_blend.py`
now asserts valid SQL **and** row-level correctness for INNER/LEFT/FULL (xfails
deleted).

## The fix that landed (`trilogy/core/models/build.py`)

INNER already gives a scoped-join source its merge-style identity + pseudonym
(own-identity build in `alias_origin_lookup` + mutual `pseudonym_map` link), so
two distinct-base rowset measures co-source and the graph stays connected. LEFT
and FULL gated that out for ROOT/ROWSET keys (`_is_binding_keyed`), trusting the
"column-partial / rowset machinery" — which only works when the canonical key
column is **physically shared** (same-base / year-over-year rowsets). Two
DISTINCT-base rowsets (`sales.region` vs `returns.region`) share no column, so:

- **LEFT** dead-ended at discovery (`DisconnectedConceptsException`).
- **FULL** rendered `INVALID_REFERENCE_BUG` (the source rowset can't produce the
  canonical column).

Fix: a new `_distinct_base_rowsets(s, t)` predicate (via `_root_base_addresses`,
which walks lineage to the terminal roots — disjoint roots ⇒ distinct base). When
it holds:

1. **LEFT** target → add to `scoped_merge_sources` (own-identity + pseudonym) but
   track in `scoped_rowset_identity_sources` and **subtract that from
   `scoped_partial_derived`**, so the rowset machinery still owns partiality (no
   double-mark) → real `LEFT OUTER JOIN`, unmatched left rows preserved.
2. **FULL** source → same identity treatment **plus** a MUTUAL `pseudonym_map`
   link (the one full-join source allowed past the `full_join_sources` skip),
   so the merge-node coalesce can render the canonical's value under the authored
   source name on canonical-only rows (the returns-only key was NULL otherwise).

Same-base rowsets are untouched (`_distinct_base_rowsets` is False) — verified by
`test_rowset_outer_join_shared_base_no_fanout` (which regressed under an
unconditional version and pins the shared-canonical-column path).

## Symptom

A query-scoped join between two aggregate rowsets, projecting a measure from
**both** sides, resolves and compiles under **INNER** but fails under **LEFT/FULL**:

```
with a as select sales.region as reg,    sum(sales.sales_amt)    as amt_a;
with b as select returns.region as reg,  sum(returns.return_amt) as amt_b;
select a.reg, a.amt_a, b.amt_b  <JOINTYPE> join a.reg = b.reg;
```

| join | result |
|------|--------|
| INNER | ✅ valid SQL |
| LEFT  | ❌ `DisconnectedConceptsException` — "sourced individually but not joinable: {a.amt_a, a.reg, b.amt_b}" |
| FULL  | ❌ `ValueError: Invalid reference string found in query` (emits broken SQL) |

## Scope (what is / isn't the trigger) — all verified on the test fixture

- **Join type is the only variable.** Same keys, same projections; INNER works,
  LEFT/FULL don't. So it is not "scoped joins can only enrich one side" (INNER
  blends two rowset measures fine) and not a parse/grammar issue.
- **Needs a measure from BOTH rowsets.** Projecting only one side's measure under
  LEFT resolves. The failure is the cross-rowset measure pair under an outer join.
- **Both operands must be rowsets.** A LEFT join with a *live* aggregate left
  operand + rowset right operand resolves. Trigger is rowset-vs-rowset specifically.
- **Not the same-base-key collapse.** Reproduces across two *distinct* models
  (`sales`, `returns`), so it is not the same-base 1=1 collapse from the multiselect
  conversion notes.

LEFT and FULL likely share a root in how the outer-join branch assembles the final
query from the two rowset subgraphs (the cross-component join edge that INNER adds
is missing/mis-applied), with FULL additionally mis-rendering a column reference.

## Where it came from

TPC-DS q05. `all_sales` unifies sale and return measures on one row grain, so the
correct answer needs no join (see `handoff_q05_q80_rollup_label_via_join.md`). But
the agent reasoned in TPC-DS table terms (separate `store_sales`/`store_returns`),
split `all_sales` into two aggregate rowsets, and tried to `full join` them — which
hit this bug. So this bug compounds the q05 idiom problem.

## Likely investigation start

The discovery/assembly path that handles a cross-rowset INNER scoped join vs the
LEFT/FULL branch. Compare the node graph for the INNER case (works) against LEFT
for the same fixture; the INNER plan has a join edge co-sourcing `a.amt_a` and
`b.amt_b` that the LEFT plan lacks. Related (do not conflate — those are about
derived keys / same-base collapse, this is about outer join + two rowset measures):
`test_scoped_join.py`, `test_rowset_derived_twice_join_bugs.py`,
`handoff_scoped_full_join_derived_key.md`, `project_query_scoped_full_join` (memory).

## Done = the two xfail tests pass

Delete the `xfail` markers (and the `test_left_join_currently_raises_disconnected`
pin) and assert valid SQL, mirroring the INNER assertion.
