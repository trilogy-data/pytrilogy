# Engine-bug handoffs — rebaseline run `20260706-222300`

Five real framework bugs from the >500k token-sink fan-out. Each handoff = symptom, minimal
repro, root-cause file:line, fix direction, guard test. Full diagnostics in the sibling
`bug_q*_*.md` reports; the index is `../INDEX_sinks_rebaseline_20260706.md`.

**Verify before fixing.** TWO subagent verdicts turned out FALSE on recheck: q59 ("eval reference
bug" — override already existed; real issue was a 99/100 near-miss) and q23 (mis-filed as an engine
bug off MISLEADING docs — owner ruled `?` filters the prior expression, working as designed). Treat
every verdict as a lead, not a fact — reproduce first, and confirm semantics with the owner.

| priority | handoff | class | verified | status | fix locus |
|---|---|---|---|---|---|
| — | `handoff_q87_except_intersect_column_pruning.md` | silent | subagent | ✅ FIXED | `optimizations/hide_unused_concept.py` (early-return non-UNION_ALL) |
| 1 | `handoff_q54_subset_join_rowset_anchor_full_leak.md` | silent | subagent | open | `models/build.py` `_rowset_outer_pair` ~L2400 |
| 2 | `handoff_q84_union_join_rowset_grain_collapse.md` | silent | subagent | open | group/rowset grain resolution |
| 3 | `handoff_q83_unnecessary_sales_fact_join.md` | perf | subagent | open | `processing/concept_strategies_v4.py` |
| 4 | `handoff_q30_concept_suggestion_ranking.md` | DX→silent | subagent | open | `models/environment.py:390` |

**q87 is FIXED** (early-return for non-`UNION_ALL` `UnionCTE` in `HideUnusedConcepts` — the fix this
handoff proposed; residual 28-row gap tracked separately). **q54 is now top priority** — like q87 it
sits on a path (`subset/union join`) that commit `4e69c5547`'s expanded guidance steers agents toward.

**q23 — RESOLVED, NOT A BUG** (`handoff_q23_filter_outside_aggregate_input.md`): `?` filters the
immediately-prior expression uniformly (no inline where/having split). `(sum(x) by k) ? cond` filters
the aggregate result → lifetime, by design; windowed needs `sum(x ? cond) by k`. The old
`ai/constants.py:127` / `syntax_examples.py:107` phrasing ("filters one expression's input") misled
both the agent and this investigation; owner has updated the docs. No engine change.

Not included (not engine bugs): q59 (override already exists; 99/100 near-miss), q23 (see above),
q11 (DX — name `all_sales` in disconnect error), q05/q02/q14/q64/q75/q80 (agent/guidance — see index).
