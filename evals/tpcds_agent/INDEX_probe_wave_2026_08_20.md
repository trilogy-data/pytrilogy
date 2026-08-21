# Index: 2026-08-20 probe-wave framework bugs

One row per finding from the 2026-08-20 probe wave over run
`20260820-031800`'s >500k token sinks (7 read-only probes, one per
sink/cluster). Every open report carries a minimal repro and file:line root
cause. This file is the loss-tracking ledger for the wave: when a report is
fixed and deleted, strike its row here rather than deleting it, so the wave
stays auditable end to end. Severity context and the pre-existing backlog live
in `README.md`.

## Engine bugs (silent wrong results)

| status | report | one-line |
|---|---|---|
| FIXED 2026-08-20 | `bug_q47_window_rowset_churn.md` | Two P1 codegen bugs: engine-combined conditions rendered unparenthesized (`A or B and C` reassociates), and `QueryDatasource.__add__` dropped `nullable_concepts` (LEFT OUTER silently became INNER). Regression tests in tree; non-bug polish split to `handoff_q47_diagnostic_polish.md`. |
| open | `bug_silent_ingest_sinks_q49_q59_q72_q77.md` | q72 (the run's graded FAIL): rowset boundary strips non-key nullability, plain `=` rejoin drops 404 NULL groups; plus union-join aggregate emits wrong grain. q59/q77: `_coalescing_presence_probe` phantom/vacuous family. q49: no bug (explore tooling cost). |
| open | `bug_keyless_join_guard_ingest_cluster.md` | One root cause behind all 37+3 keyless-join guard firings: aliased outputs give ROOTs rename-only lineage reach, the co-source bucket test misses one-FK-hop relatedness, dim attr splits into a keyless bucket. The no-fact-key variant still ships a silent `on 1=1` cartesian. ESCALATED 2026-08-20: run 153007 enriched_docs q81 (915k raw) is the cluster's first graded WRONG ANSWER - the guard rejected a correct aliased query 3x and the forced workaround flipped a row (details in the report's update section). |
| open | `bug_q66_union_output_drops_nullable.md` | Filed from the 153007 triage (both legs failed q66 identically): `union(...)` TVF output concepts never inherit arm nullability (`parsing/common.py:1638/1701`), so sibling filtered-aggregate rejoins render plain `=` on a NULL group key and silently drop the row. Minimal repro = two measures + nullable union key; explicit `?` in the union signature restores the row. |

## Planner/renderer bugs (loud, but wrong or blank errors)

| status | report | one-line |
|---|---|---|
| open | `bug_q44_empty_unexpected_error.md` | `InlineDatasource` folds a dim scan the broadcast join needs; bare `AssertionError` surfaces as a COMPLETELY EMPTY "Unexpected error" message (CLI renders `str()` of it), so the agent retried blind six times. |
| open | `bug_q14_values_list_virt_filter_binder.md` | Filtered aggregate over a KEY silently drops its filter mask below the dedup GroupNode; the union escape hatch then emits invalid SQL with phantom `_virt_filter_*` columns instead of the clean missing-source error. |
| open | `bug_q17_join_condition_syntax_loop.md` | `detect_join_missing_key` fires Syntax [225] "Expected a join condition" on post-select joins that are only missing the trailing `;`; the correct 202 message never surfaces. Drove 3M raw tokens across two legs. |

## Diagnostics polish (split out by the fix pass)

| status | report | one-line |
|---|---|---|
| open | `handoff_q47_diagnostic_polish.md` | Leaf-shorthand spelling missing from did-you-mean suggestions, plus the q29-found ranking defect (a statement's other undefined refs pollute the suggestion pool) and one-at-a-time scoped-join key errors. |

## Not framework bugs (do not re-chase)

| status | report | one-line |
|---|---|---|
| open (question fix) | `bug_q29_cross_leg_sink.md` | Sank in all three legs because the QUESTION omits the sale-to-return match keys; proposed rewording inside. Harness lesson: raw token counts were ~10x inflated by reasoning-replay cache hits; the >500k detector should read cache-adjusted (fresh) tokens. |

## Same-session related work

- `docs/explore_compact_output_design.md` - explore v3 outline-by-default
  (landed same day): the token-side fix for the q49-class churn.
- `evals/EVAL_LOOP_INSTRUCTIONS.md` - new `enriched_docs` eval category
  (language reference preloaded in-task), splitting language-discovery cost
  from authoring cost in the funnel.
