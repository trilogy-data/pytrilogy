# Handoff (part 2): finish removing the v3 fallback — q64 + recursive

## Status

The **rowset port is DONE** and the **fallback audit is 0 hits** across the
75-query test set (was: rowset 24, multiselect 1). Test set is **74/75** —
only **q64** mismatches (executes, wrong rows). `ruff`/`mypy`/`black` clean.

Validate at any time:
```
.venv/Scripts/python.exe local_scripts/discovery_v4_compare.py --test-set
.venv/Scripts/python.exe local_scripts/v4_fallback_audit.py   # expect Totals: {}
```
Per-query reports: `local_scripts/v4_compare/queryNN.md`, summary `SUMMARY.md`.

## What was done (part 1)

### Rowset — fully native (`_GENERATORS['rowset'] = gen_rowset`)
- **Removed the incomplete inner-walk** in `v4_helper/concept_graph.py`
  (`build_concept_graph`): a ROWSET concept is now a true leaf in the outer
  graph. It only ever captured inner outputs + WHERE, never inner HAVING /
  multiselect arms, and the condition-phase/nested rowsets (q14/23/35/44/54/69)
  were never actually exercised natively — they all fell back to v3.
- **`gen_rowset` (`v4_node_generators/rowset.py`) → `resolve_rowset`
  (`concept_strategies_v4.py`)** recursively plans the rowset's inner select
  through v4 `search_concepts` (mirrors v3 `get_query_node`):
  - fresh build env + graph per inner select (the OUTER env classifies the
    inner's concepts under rowset aliasing — a plain root reads back as
    `derivation=rowset` there — so reusing it mis-buckets; q14 nested rowsets);
  - inner WHERE applied; inner HAVING applied as a post-aggregate `SelectNode`,
    **cross-joining** any separate scalar rowset the HAVING references
    (q14 `bucket_sum_l0 > avg_sales.average_sales`);
  - boundary projection re-exposes the producer under the outer handles;
    exposes the demanded handles + any **pseudonym-carrying** derived handle so
    a cross-rowset `merge … into` (q44/q54) joins on the merged keys via
    `get_node_joins`' canonical-pseudonym map (instead of `1=1`); sets `grain`.
- **q44 (d1-aggregate filter scoping)** — `group_graph.py`
  `_propagate_raw_filters_to_d1_roots`: a `root_d1` pristine scan inherits a
  sibling main-root's raw filter ONLY when (a) it feeds no existence/semijoin
  source AND (b) one datasource carries both the filter's columns and the
  d1-root's scan columns (datasource colocation, threaded in as
  `datasource_columns`). Distinguishes q44 (`store=1` belongs on
  `avg(net_profit) by item` — same `store_sales` table) from q06 (`date` must
  NOT touch `avg(item.price) by category` — different table) and q02 (semijoin).
- **Multiselect rowsets unified**: `_add_concept` now sets `rowset_name` for
  ALL rowset items (not just plain SELECT). `group_rules.partition_rowsets`
  buckets every handle of one rowset into ONE boundary group keyed by
  `(scope, rowset_name)` — deliberately NOT depth or the `@condition` phase, so
  a rowset used in both SELECT and WHERE stays one group (q64 needed this so an
  arm's `count(...)` and its per-arm `marital != …` filter land together).
- **Boundary applies injected conditions**: `resolve_rowset(conditions=…)`
  wraps the boundary in a `SelectNode` so a consumer-side predicate over the
  rowset's rows (q64 arm's `marital != …`) is actually applied.
- **Rowset-wrapped multiselect (q64)**: the boundary carries the multiselect's
  align ARM concepts (`s_name_99/00`) as HIDDEN outputs so the renderer's
  `BuildMultiSelectLineage.find_source` resolves the aligned concept (`s_name`)
  in the boundary CTE; outer CTEs then reference the materialized column.

### Key files touched
- `trilogy/core/processing/v4_node_generators/{rowset.py,dispatch.py}`
- `trilogy/core/processing/concept_strategies_v4.py` (`resolve_rowset`, imports,
  `datasource_columns` passthrough)
- `trilogy/core/processing/v4_helper/concept_graph.py` (removed inner-walk;
  `rowset_name` for all rowsets)
- `trilogy/core/processing/v4_helper/group_graph.py`
  (`_propagate_raw_filters_to_d1_roots`, `build_group_graph` signature)
- `trilogy/core/processing/v4_helper/group_rules.py` (`partition_rowsets`)

## REMAINING WORK

### 1. q64 — cross-arm COUNT coalesced to 0 (the only failing query)

q64 is a rowset (`q64_results`) wrapping a **multiselect** (two year-slice arms,
align on `item_sk/s_name/s_zip`, per-arm `marital != …`, outer cross-arm
`cnt_00 <= cnt_99`). It now plans **entirely natively** and executes, but
returns **34 rows vs 2** (fan-out).

Root cause (confirmed): the multiselect merge CTE FULL-joins the two arms and
**coalesces the cross-arm COUNT aggregates to 0**:
```sql
coalesce("courageous"."_q64_results_cnt_00",0) as "_q64_results_cnt_00",
...
coalesce("puzzled"."_q64_results_cnt_99",0)  as "_q64_results_cnt_99"
```
This comes from the renderer rule at **`trilogy/dialect/base.py:1146`** — a
nullable `COUNT` gets `coalesce(..., 0)` (correct for sparse dim enrichment
through a LEFT/FULL join). For q64 it's wrong: a single-year item gets
`cnt_00 = 0` instead of NULL, so the outer `cnt_00 <= cnt_99` (`0 <= N` = true)
keeps it, instead of being excluded by `NULL <= N` = NULL/false.

**The reference (v3) keeps these NULL.** Its structural difference: v3 does NOT
build an intermediate coalescing merge CTE — it FULL-joins the two arm CTEs at
the OUTER level (`busy.cnt_00 <= kaput.cnt_99`, each referenced from its own arm
CTE where it's non-null). v4 materializes the multiselect merge inside the
rowset boundary (one CTE), which is where the coalesce fires.

Fix directions (pick one; verify against q05/q46 — the other multiselect
queries — and the full set):
- (a) Don't mark the cross-arm align aggregates nullable in the multiselect
  `MergeNode` (`_resolve_multiselect`), so `base.py:1146` doesn't trigger. Risk:
  other FULL-join nullability.
- (b) Narrow the `base.py:1146` COUNT-coalesce so it doesn't apply to a
  multiselect-align FULL join (the NULL is semantically load-bearing there).
- (c) Apply the cross-arm HAVING (`cnt_00 <= cnt_99`) at/inside the merge,
  before the coalesce, where both raw arm counts are present.

Repro/inspect:
```
.venv/Scripts/python.exe local_scripts/discovery_v4_compare.py --query 64
# read local_scripts/v4_compare/query64.md (v4 SQL vs reference SQL)
```
Earlier q64 layers already solved (don't regress): gen-time `find_source`
("could not find upstream map for multiselect s_name") via hidden arm concepts;
the per-grain fragmentation via `(scope, rowset_name)` unification; the missing
arm aggregation via applying the boundary's injected `marital !=` condition.

### 2. Recursive — untouched, zero coverage

No test-set query exercises `Derivation.RECURSIVE`, so its fallback can't be
removed untested. Per part-1 plan: author a `.preql` with a recursive CTE, add
it to `local_scripts/v4_compare/test_set.txt`, confirm it currently works via
the v3 fallback, then write `gen_recursive`
(`v4_node_generators/recursive.py`, mirror `node_generators/recursive_node.py`),
register it in `dispatch._GENERATORS`, and confirm parity.

### 3. Final cleanup (ONLY after q64 passes and recursive is native)

- `dispatch.py`: delete `_fallback_to_v3` + the `_v3_build_node_for_group`
  import; the `fn is None` branch goes away (unknown derivation should raise).
- Delete `trilogy/core/processing/v4_helper/factory_dispatch.py` and its dead
  v3 `gen_*_node` imports. NOTE: the audit already shows **0 fallback hits**, so
  the fallback is effectively dead code for the test set today — but keep it
  until recursive is native (and q64 green) so nothing silently breaks.
- Leave `node_generators/*_node.py` (v3 + TPC-DS pytest suite still use them).

## Validation checklist
- `discovery_v4_compare.py --test-set` → 75/75 match, no gen/exec/ref fail.
- `v4_fallback_audit.py` → `Totals: {}`.
- `ruff check trilogy && mypy trilogy && black trilogy tests`.
- `tests/core/processing/test_v4_*.py`.
