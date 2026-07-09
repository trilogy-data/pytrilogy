# Engine-bug handoffs — rebaseline run `20260706-222300`

Five real framework bugs from the >500k token-sink fan-out. Each handoff = symptom, minimal
repro, root-cause file:line, fix direction, guard test. Full diagnostics in the sibling
`bug_q*_*.md` reports; the index is `../INDEX_sinks_rebaseline_20260706.md`.

**Verify before fixing.** TWO subagent verdicts turned out FALSE on recheck: q59 ("eval reference
bug" — override already existed; real issue was a 99/100 near-miss) and q23 (mis-filed as an engine
bug off MISLEADING docs — owner ruled `?` filters the prior expression, working as designed). Treat
every verdict as a lead, not a fact — reproduce first, and confirm semantics with the owner.

Status as of 2026-07-08: four of the five are closed; the resolved handoff files have been
deleted. Only **q83** remains open.

| priority | handoff | class | verified | status | fix locus |
|---|---|---|---|---|---|
| 1 | `handoff_q83_unnecessary_sales_fact_join.md` | perf | subagent | open | `processing/concept_strategies_v4.py` |

Closed since this rebaseline (handoff files removed):

- **q87 — ✅ FIXED**: early-return for non-`UNION_ALL` `UnionCTE` in `HideUnusedConcepts`
  (`optimizations/hide_unused_concept.py`); residual 28-row gap tracked separately.
- **q54 — ✅ FIXED** (2026-07-08): filtered-rowset-anchor subset/left join now narrows
  directionally via `_rowset_definition_boundary` (`optimizations/value_set_join_upgrade.py`),
  gated on strict SUBSET. The earlier "not a bug" refutation was overturned.
- **q84 — NOT A BUG** (refuted): subagent misread Trilogy syntax; `--` is a HIDE modifier, not a
  comment, and datasource/rowset forms are symmetric. Parity guards in
  `test_duckdb_union_join_rowset_grain.py`.
- **q30 — ✅ FIXED**: `_find_similar_concepts` ranked path matches by dict-insertion order, burying
  the correct shallow match; now sorts by `(extra_segments, subsequence_gaps)`
  (`models/environment.py`). Guards in `test_undefined_concept.py`.
- **q23 — RESOLVED, NOT A BUG**: `?` filters the immediately-prior expression uniformly.
  `(sum(x) by k) ? cond` filters the aggregate result → lifetime, by design; windowed needs
  `sum(x ? cond) by k`. Misleading docs (`ai/constants.py` / `syntax_examples.py`) updated by owner.
  No engine change.

Not included (not engine bugs): q59 (override already exists; 99/100 near-miss),
q11 (DX — name `all_sales` in disconnect error), q05/q02/q14/q64/q75/q80 (agent/guidance — see index).
