# handoff: two diagnostic-polish items split out of bug_q47

Filed: 2026-08-20, when `bug_q47_window_rowset_churn.md` was deleted on both of its
P1 codegen bugs being fixed. Neither item below is a correctness bug; both are agent-cost
items observed in run `results/20260820-031800_enriched_deepseek_deepseek-v4-flash` (q47),
and neither was addressed by that fix.

## 1. Rowset did-you-mean never offers the leaf shorthand

`rs.col` leaf shorthand resolves at parse time when unambiguous
(`project_rowset_output_shorthand_resolution`). The agent wrote an invented flattened
spelling, `monthly_totals.store_name`; the undefined-concept error and its suggestion were
both correct, but the suggestion list offered only the full dotted path
(`monthly_totals.ss.store.name`) and never the shorter valid leaf
(`monthly_totals.name`).

Verified at the time of filing: `monthly_totals.name` and `monthly_totals.ss.store.name`
both resolve and run; `monthly_totals.store_name` is not the leaf of any output.

Fix direction: when building the suggestion list for a rowset-qualified reference, include
the unambiguous leaf spelling alongside the dotted path, so the message matches the idiom
the docs teach.

## 2. `window_filter_needs_having` fires on the intentional bookend pattern

q47's shape is deliberate: pull an extra month on each side of the reporting window in the
WHERE so `lag`/`lead` have neighbours, then narrow to the reporting year in the HAVING. The
warning fired twice on a body that was already doing exactly what it asks for, and is what
nudged the agent into the rowset (`with`) rewrite that then exposed the row-universe fork.

Fix direction: suppress the warning when the filtered concept is already constrained by a
HAVING atom, or when the window's ordering concepts are the ones the WHERE widens.
