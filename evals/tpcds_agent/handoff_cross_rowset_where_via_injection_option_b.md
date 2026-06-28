# Handoff: cross-rowset WHERE — "Option B" (injection trigger) as an alternative to the shipped fix

## Status
The q11/q23 cross-rowset-WHERE bug is **FIXED** (Option A, see below). This handoff
captures **Option B**, an alternative approach the maintainer wanted explored. Option B
as prototyped does **not** resolve the bug on its own; it needs a `gen_rowset_node`
change too. The open question is whether a cleaner end state routes through Option B's
injection + a small enrichment tweak instead of Option A's dedicated branch.

## The bug (recap)
`select store_2001.cust_id` over four scoped-joined rowsets (store/web × 2001/2002)
with `where web_2002.rev > store_2002.rev and web_2001.rev > store_2001.rev`.
The WHERE operands belong to rowsets *other than* the output's rowset and appear
**only** in the WHERE. The discovery loop sources only `store_2001` (the output's
rowset), then `validate_stack` → `INCOMPLETE_CONDITION` →
`check_for_early_exit` raises `SyntaxError: Have {RowsetNode<store_2001...>} and need ...`.
Single-comparison and the full agent query11 *silently drop* the filter (wrong results)
instead of erroring. Deterministic repro + expected rows in
`tests/test_scoped_join_cross_rowset_multi_where.py`.

## Option A (SHIPPED)
`trilogy/core/processing/node_generators/rowset_node.py`, `gen_rowset_node`:
when `conditions` reference concepts not in the rowset's own outputs
(`condition_targets`), source `[concept] + local_optional + condition_targets` as a
single merge (with `conditions=None`, to avoid re-entering this branch per operand
rowset), then `add_condition` + `set_preexisting_conditions` on the merge and return it.
The `MergeNode`'s own join inference connects the rowsets on the shared scoped-join key
(`store_2001.cust_id = web_2002.cust_id`, via pseudonym equivalence). Verified SQL is a
single join of all four rowsets on cust_id with both atoms in the WHERE; passes the new
test + the 6 tests Option A's earlier (too-broad) prototype had regressed
(q23/q64/q72/q85 + two non-benchmark) + `tests/test_scoped_join*` + `tests/core/processing`.

## Option B (PROTOTYPE — insufficient alone)
Idea (maintainer's): a rowset output is a self-contained materialized subquery, so once
**only rowsets (or materialized roots)** remain to source, the condition can't be pushed
down further and its inputs should be **injected into the candidate list** — the existing
"all remaining mandatory concepts are materialized roots" path.

Change tried, in `trilogy/core/processing/discovery_utility.py`,
`_resolve_condition_disposition`, the `all_materialized_roots` predicate:
```python
all(
    x.derivation in (Derivation.ROOT, Derivation.ROWSET)
    and x.granularity != Granularity.SINGLE_ROW
    and (
        x.derivation == Derivation.ROWSET
        or x.canonical_address in materialized_canonical
    )
    for x in remaining
)
```
(Rowsets aren't in `materialized_canonical`, so they bypass that sub-check.)

### Why it's not enough
The injection **does** fire (log: "All remaining mandatory concepts are materialized
roots, injecting condition inputs into candidate list"). The operands then reach
`gen_rowset_node` as `local_optional`, and it bails:
```
[GEN_ROWSET_NODE] no possible joins for rowset node to get
   ['web_2002.rev','store_2002.rev','web_2001.rev','store_2001.rev'];
   have ['store_2001.cust_id','store_2001.rev']
```
…so it returns the bare node and the same `INCOMPLETE_CONDITION` error is raised.

### Root reason (the interesting bit)
The join key `store_2001.cust_id` **is right there** in `node.output_concepts`, but
`possible_joins` is computed as
```python
concept_to_relevant_joins(
    [x for x in node.output_concepts if x.derivation != Derivation.ROWSET]   # drops cust_id
    + [canonical for _, canonical in bridge_keys]                            # _pseudonym_bridge_keys
)
```
It **excludes ROWSET-derived outputs** and only recovers a key via a *non-rowset* dim
bridge (`_pseudonym_bridge_keys`: a rowset FK collapsed onto a dim key). Here
`store_2001.cust_id`'s only pseudonyms are the **other rowsets'** cust_ids (all ROWSET,
no non-rowset bridge) → key filtered out → "no possible joins". The legacy enrichment
path is built for rowset→dim joins, not rowset→rowset.

## Open question for the next agent
Is there a cleaner unified design where Option B's injection delivers the operands and
`gen_rowset_node` **uses the rowset key already in its outputs** to join the operand
rowsets (relax the `derivation != ROWSET` filter / bridge requirement for the
scoped-join case), instead of Option A's separate single-merge branch? Watch for:
- The enrichment path re-sources the rowset as a join key → **double CTE / FULL JOIN
  on 1=1** (the first Option A prototype hit exactly this: `enrich_node` exposed only
  the operand revs, no cust_id, so the outer merge cross-joined). Option A sidesteps it
  by sourcing the output and operands together as one merge.
- Don't broaden `all_materialized_roots` to ROOTs without the `materialized_canonical`
  guard — only the ROWSET arm should skip it.
- The earlier "force rowset condition inputs into the mandatory list" approach
  (`initialize_loop_context`) fixed q11 but **regressed 6 tpc-ds tests** (too broad — it
  fired on single-source rowset conditions handled fine by pushdown). Avoid.

## Repro / verification
```
.venv/Scripts/python.exe -m pytest tests/test_scoped_join_cross_rowset_multi_where.py -q
```
Regress set: `tpc_ds_duckdb/test_queries.py::{test_twenty_three,test_sixty_four,test_seventy_two,test_eighty_five}`
+ `test_non_benchmark_queries.py::{test_q64_rowset_join_with_second_fact_join_hoist,test_property_via_partial_fk_does_not_broadcast}`.
