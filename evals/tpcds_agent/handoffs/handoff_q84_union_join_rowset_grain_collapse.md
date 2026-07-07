# Handoff — q84: `union join` onto a rowset drops the rowset's grain columns from GROUP BY (silent dedup)

**Verification: ❌ REFUTED 2026-07-06 — NOT A FRAMEWORK BUG.** The subagent report misread Trilogy
syntax; the datasource/rowset asymmetry does not exist. Parity guards landed in
`tests/engine/test_duckdb_union_join_rowset_grain.py`. Original (faulty) report kept below for the
record; detail report: `bug_q84_regression_20260706.md`.

## Why the report was wrong
The "passing" direct-join query (`results/20260706-135542_enriched/workspace/query84.preql`) contains:

```
    --sr.ticket_number as ticket,
    --sr.item.id as item_id
```

`--` is Trilogy's **select-item hide modifier** (`select_hide_modifier` in `trilogy.lark:280`), NOT a
comment. The old query explicitly selected the returns grain as hidden outputs — THAT is what put
`SR_ITEM_SK`/`SR_TICKET_NUMBER` into the select grain and the final `GROUP BY`, preserving the fan-out
(16 rows). There is no automatic "a joined datasource contributes its physical grain" behavior:

| form | authored grain outputs | rows |
|---|---|---|
| direct `union join` to store_returns | hidden (`--`) ticket+item | **16** ✅ |
| direct `union join` to store_returns | none | 100+ (join collapses to customer side entirely) |
| `union join` to rowset return_demos | none | 15 (dedup to select grain — the "failing" run) |
| `union join` to rowset return_demos | hidden (`--`) ticket+item | **16** ✅ |
| `union join` to rowset return_demos | visible ticket+item | **16** ✅ |

Both forms behave identically for identical authored outputs: no grain outputs → dedup to the select
grain (documented Trilogy semantics: select outputs define grain); grain outputs (hidden or visible) →
fan-out preserved. The rowset form even survives the planner pruning the hidden `item_id` column out of
intermediate CTEs (guarded: the two fan-out returns differ only by item).

## Classification
Agent authoring variance, not a framework defect. The new run's agent dropped the grain-preserving
hidden outputs the old run's agent had authored; 15 rows is the correct answer to the Trilogy it wrote.
Possible follow-up is guidance-side: the row-multiplicity idiom (`--hidden` grain outputs to preserve
fan-out under the select-grain dedup) is what TPC-DS bag-semantics references need.

## Guard tests (landed, all passing)
`tests/engine/test_duckdb_union_join_rowset_grain.py` — 5-cell parity matrix (rowset vs direct ×
no-grain/hidden-grain, + visible-grain), q84 data shape (one customer matching two returns that share a
ticket and differ only by item).

---

## Original report (refuted)

A scoped `union join` onto a ROWSET drops the rowset's non-referenced grain columns from the final
`GROUP BY` (emits `GROUP BY 1,2` = only the anchor/customer fields), silently deduping a legitimate
fan-out. Joining the base DATASOURCE directly keeps the grain columns and preserves the fan-out.
- q84: ref 16 rows / cand 15 — drops one fan-out duplicate ("Benson, Floyd", whose demographic matches
  two store-returns tickets). SILENT.
- Trigger matrix: direct-datasource join → 16; rowset join → 15; rowset join + SELECTing the rowset's
  ticket/item → 16. Minimal failing combo = scoped join to a rowset whose grain columns aren't in the
  SELECT output.
  [REFUTATION NOTE: the "direct-datasource join → 16" cell was run with the hidden `--` select items
  still present; without them it returns 100+ rows, not 16.]

### Root cause (locus) — [refuted]
Grain resolution divergence between datasource-grain preservation and rowset-grain: select grain from
outputs is identical both forms (`select_finalize.py:239-253`); the divergence is downstream in
group/merge grain — `trilogy/core/processing/node_generators/group_node.py` (`target_grain`) and rowset
grain in `trilogy/core/processing/nodes/...rowset_node.py`. A datasource contributes its grain keys to
the GROUP BY even when unreferenced; a rowset does not. No single fix line — trace where the rowset's
grain keys are dropped from the group node's grain.

### Not a regression
Pre-existing: the NEW (rowset-form) query returns 15 on the parent commit `1c7fed75a` too. The pass→fail
is agent authoring variance (direct-join last run → rowset form this run), plausibly nudged by the
commit's expanded rowset/set-op guidance.
