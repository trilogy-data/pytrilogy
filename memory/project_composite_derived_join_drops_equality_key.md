---
name: project_composite_derived_join_drops_equality_key
description: FIXED 2026-06-30 — cross-rowset scoped join combining a plain-equality key with a derived-expression key silently DROPPED the equality co-key → fan-out; regression from the derived-join spike, now co-keys sourced onto the enrich side
metadata:
  type: project
---

FIXED 2026-06-30. Was SILENT WRONG RESULTS: a cross-rowset scoped join ANDing a plain-equality key with a derived-expression key emitted ONLY the derived key; the equality co-key vanished → fan-out across all values of the dropped key. `inner join a.store=b.store and a.period+10=b.period` rendered `ON a.period+10=b.period` (store gone). Both the composite `and` form AND two-separate-clause form dropped it. Two-plain-equality composites kept both keys, so trigger = equality-key + derived-key in one cross-rowset join.

Discovered on q59 (store×week, this-year vs `week_seq+52`): store_code dropped → 2548 rows not ~260. Was likely root of rebaseline fan-outs where cand≫ref: q64 (2→595), q73 (1→82), q23 (4→100), q66, q84, q17, q77 — worth re-checking those.

Root cause: `_enrich_via_derived_join_key` (`node_generators/rowset_node.py`) sourced the enrich node with only the derived key's OTHER side (`b.period`) + optionals — never the equality co-key (`b.store`). So the enrich datasource never exposed `b.store` and `get_node_joins` (which infers joins purely from shared CANONICAL addresses) related the two rowsets on the derived key alone.

FIX: after building `other_keys`, also compute co-keys = `scoped_inner_join_keys | scoped_full_join_keys | scoped_left_anchor_keys` MINUS {derived other-side addrs, derived key addrs, derived key pseudonyms}, add them to the enrich `source_concepts` mandatory_list, and UN-HIDE any anchor-side (`node`) output whose address/pseudonym matches a co-key. Both sides then expose the co-key (sharing a canonical via the scoped-join pseudonym) → inferred join carries every authored key. No explicit join-condition construction needed; the canonical-address inference does it once both columns are present.

Confirmed: `repro_composite_derived_join_drops_equality_key.py` (4→2 rows, store_in_join True) + `tests/test_scoped_derived_rowset_join_matrix.py::test_composite_mixed_key_inner_join_keeps_equality_co_key`. Family: [[project_left_derived_rowset_join_recursion]], [[project_q02_derived_join_key_no_connectivity_edge]]. Guardrails green: test_scoped_join.py, test_rowset_generation_matrix.py, q29, q78.
