---
name: project_left_derived_rowset_join_recursion
description: LEFT scoped join on a derived-expression key between two re-aggregated rowsets recurses forever (uncaught RecursionError); regression from the derived-rowset-join spike's scoped_left_anchor_keys path
metadata:
  type: project
---

LEFT scoped join on a **derived-expression** key (`a.k + N = b.k`) between two rowsets whose measures are re-aggregated in the final select → uncaught `RecursionError`, surfaced as opaque `Unexpected error: maximum recursion depth exceeded`. INNER join (same shape) resolves; LEFT with plain-equality key resolves; LEFT + derived key is the failing intersection. Not union/filter-rowset/multi-rowset specific (those just enlarged the discovered instance — TPC-DS q02 self-join attempt, drove one run to 2.3M tokens/50 iters).

**Regression from the in-flight join spike**: recursion cycle runs through `_enrich_via_derived_join_key` (`node_generators/rowset_node.py:463`), which does NOT exist in HEAD (uncommitted spike code). Its docstring asserts "the other rowset is never re-sourced through this one (which would recurse)" — true for `scoped_inner_join_keys`, FALSE for `scoped_left_anchor_keys`: `_producible_derived_join_keys` (rowset_node.py:107) scans the left-anchor registry too, so the LEFT derived key enters the same enrichment, but sourcing the other side's key re-enters `_generate_rowset_node` for the same rowset → `_enrich_rowset_node` → `_enrich_via_derived_join_key` with no history/visited guard.

Cycle: search_concepts→_search_concepts→generate_node→_generate_rowset_node→gen_rowset_node→_enrich_rowset_node→_enrich_via_derived_join_key→source_concepts→_generate_basic_node→...→_generate_rowset_node (loops).

**Fix direction**: (1) terminate — exclude scoped_left_anchor_keys from `_producible_derived_join_keys` OR thread a history/visited guard so the other side's key sources without re-entering this rowset (ideally make LEFT resolve like INNER); (2) discovery must raise a clean UnresolvableQueryException on recursion depth, never let RecursionError escape as "Unexpected error" (same class as [[project_q05_rowset_over_union_rowset_recursion]]).

Sibling/guardrail: the INNER+disconnect half was the prior fix [[project_q02_derived_join_key_no_connectivity_edge]] (`repro_derived_rowset_join.py`). Handoff + generic repro: `evals/tpcds_agent/bug_left_derived_rowset_join_recursion.md` + `repro_left_derived_rowset_join_recursion.py`. Must-not-regress: test_rowset_offset_join_contract.py, q78 (scoped_left_anchor_keys), q29.
