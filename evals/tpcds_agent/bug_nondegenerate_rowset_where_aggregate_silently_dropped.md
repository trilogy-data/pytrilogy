# Non-degenerate bare WHERE aggregate over a rowset column silently dropped (no WHERE/HAVING emitted)

**Found:** 2026-07-05, while writing guards for
`bug_q23_grainless_scalar_max_fused_into_source_rowset_grain.md`. Pre-existing on that bug's
baseline (verified: the new degenerate-co-grain path never fires for this shape).
**Class:** FRAMEWORK bug, SILENT wrong results — the filter vanishes from the generated SQL.
**Status:** OPEN. Strict-xfail repro:
`tests/test_where_scalar_aggregate_degenerate_cograin.py::test_non_degenerate_rowset_where_aggregate_keeps_select_grain`.

## Repro
```
key cust_id int;
key txn_id int;
property txn_id.amount float;
property txn_id.txn_cust int;
datasource txns (t: txn_id, a: amount, c: txn_cust) grain (txn_id)
query ''' select 1 t, 60.0 a, 1 c union all select 2 t, 40.0 a, 1 c union all
select 3 t, 500.0 a, 2 c union all select 4 t, 50.0 a, 3 c ''';

with per_txn as
select txn_id as tid, txn_cust as tcust, amount as amt;

select per_txn.tcust
where sum(per_txn.amt) > 250.0
order by per_txn.tcust;
```
Expected: `[(2,)]` (cust totals 100/500/50; bare WHERE aggregate co-grains to the select grain
`{tcust}`, HAVING-like). Actual: `[(1,), (2,), (3,)]` — the emitted SQL is a bare
`SELECT c ... GROUP BY 1` with **no WHERE or HAVING at all**; the condition is silently discarded.

## Notes
- The identical shape over a plain datasource property (no rowset wrap) works correctly:
  `select txn_cust where sum(amount) > 250.0` filters as expected (guard test in the same file).
- So the drop is specific to the WHERE aggregate's input being a ROWSET output at a grain finer
  than the select grain. Same silent-filter-drop family as
  `project_constant_condition_where_dropped` / `project_is_scalar_only_condition_guard`
  (discovery_validation.py condition-requirement exemptions) — likely the condition-required
  check exempts the node once the rowset aggregate's sourcing collapses, but NOT yet diagnosed.
