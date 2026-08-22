# Index: 2026-08-20 probe-wave framework bugs

One row per finding from the 2026-08-20 probe wave over run
`20260820-031800`'s >500k token sinks (7 read-only probes, one per
sink/cluster). Every open report carries a minimal repro and file:line root
cause. This file is the loss-tracking ledger for the wave: when a report is
fixed and deleted, strike its row here rather than deleting it, so the wave
stays auditable end to end. Severity context and the pre-existing backlog live
in `README.md`.

Audited 2026-08-21 against `84bc9ffeb`: every row's repro was re-run or its
regression gate executed. Six of the eight bug rows are fixed, and their six
reports were deleted the same day (the rows below are the record; the bodies
are in `git log --diff-filter=D`). The two q47 diagnostic-polish items were
fixed and deleted 2026-08-21 as well. What remains open is the q72
`_aggregate_axis_members` residual plus the q59/q77 presence-probe family and
the q29 question/harness work. The
surviving report was rewritten down to those two open items and renamed
`bug_presence_probe_no_ops_and_q72_axis_residual.md`; both of its repros now run
on committed models rather than a copy of the run workspace.

## Engine bugs (silent wrong results)

| status | report | one-line |
|---|---|---|
| open (residual) | `bug_presence_probe_no_ops_and_q72_axis_residual.md` | Was `bug_silent_ingest_sinks_q49_q59_q72_q77.md`; rewritten 2026-08-21 to the two open items. q72 Bug A (rowset boundary stripped non-key nullability, 404 NULL groups dropped) and four of Bug B's five sites FIXED 2026-08-20; the residual is `concept_graph._aggregate_axis_members` keeping a relation member in the branch grain when the COUNTED ROW IDENTITY holds one, so `week_seq` still repeats. q59/q77 presence-probe family still open: a null test on a coalescing join-key member renders POST-merge on the fused column and no-ops on one side. q49 closed, never an engine bug (explore v3 landed). |


## Planner/renderer bugs (loud, but wrong or blank errors)

| status | report | one-line |
|---|---|---|


## Diagnostics polish (split out by the fix pass)

| status | report | one-line |
|---|---|---|
| ~~open~~ FIXED 2026-08-21 | ~~`handoff_q47_diagnostic_polish.md`~~ | Both items landed; report deleted, fix summary in `README.md`. (1) `_find_similar_concepts` now offers a rowset output's leaf shorthand (`rs.col`) behind its full path, withheld when a sibling output makes it ambiguous. (2) `window_filter_needs_having` is suppressed on the bookend shape; corpus footprint is exactly q47 and q57. The q29-found suggestion-ranking defect and the one-at-a-time scoped-join key errors this row used to claim were never in that file; the ranking defect lives in `bug_q29_cross_leg_sink.md` and stays open. |

## Not framework bugs (do not re-chase)

| status | report | one-line |
|---|---|---|


## Same-session related work

- `docs/explore_compact_output_design.md` - explore v3 outline-by-default
  (landed same day): the token-side fix for the q49-class churn.
- `evals/EVAL_LOOP_INSTRUCTIONS.md` - new `enriched_docs` eval category
  (language reference preloaded in-task), splitting language-discovery cost
  from authoring cost in the funnel.
