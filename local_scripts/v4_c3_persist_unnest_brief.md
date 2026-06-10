# C3 handoff — v4 persist-of-unnest reuse (demo_e2e)

Self-contained brief for an agent picking up the last merge-adjacent v4 failure.
Read this top to bottom; everything you need is here.

## The failing test

`tests/engine/demo/test_demo_duckdb.py::test_demo_e2e` (currently in
`tests/v4_known_failing.py` as `_RESULT`). Run it:

```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/engine/demo/test_demo_duckdb.py::test_demo_e2e" --runxfail -q -p no:cacheprovider -o addopts=
```

What it does (paraphrased):
```
auto passenger.split_cabin <- unnest(split(passenger.cabin, ' '));
persist cabin_info into dim_cabins from select passenger.id, passenger.split_cabin;
select passenger.split_cabin;            # works (raw_data still present)
<delete raw_data datasource>
select passenger.split_cabin;            # MUST still work, now only dim_cabins has it
```

v3 passes. v4 raises on the final select:
`NoDatasourceException: No datasource exists for root concept
passenger.cabin@Grain<passenger.id> ... no resolvable pseudonyms`
(`select_node.py:55`).

## Root cause

After `raw_data` (the base titanic table, binds `passenger.cabin`) is deleted,
the only source for `split_cabin` is the persisted `dim_cabins` table, which
materializes it directly. But v4 still treats `split_cabin` as a DERIVED concept
(`unnest(split(cabin))`) and tries to RE-DERIVE it from `passenger.cabin` — whose
source is gone → NoDatasource.

v4 should instead read `split_cabin` straight from `dim_cabins` (a materialized
root scan), exactly like v3.

The machinery for "read a precomputed concept from a table instead of re-deriving"
is `_materialized_root_addresses` in
`trilogy/core/processing/concept_strategies_v4.py` (~line 533). It returns the
addresses of demanded derived concepts a datasource materializes; the planner then
scans the table instead of deriving. **It explicitly excludes UNNEST** (line ~571):
```python
if concept.derivation not in (Derivation.AGGREGATE, Derivation.BASIC):
    continue
```
with the comment that row-shaping derivations (UNNEST/ROWSET/...) "generate or drop
rows the datasource scan wouldn't reproduce — e.g. an unnest merged onto a key
(`merge orid into ~orid_2`) is exposed by the table as that key but really spans
more rows." That exclusion is why `split_cabin` stays derived.

## The safe vs unsafe distinction (the whole fix hinges on this)

The exclusion is conservative because of ONE unsafe shape: a table that exposes the
unnest value **as a coarser KEY** (the table grain is the key, NOT the unnest
output) — scanning the key column does NOT reproduce the per-unnest-value rows.

The SAFE shape (this test): the persisted table is at the **post-unnest grain** —
one row per unnest value. Concretely (verified):
- `passenger.split_cabin`: `derivation=UNNEST`, `grain=[passenger.split_cabin]`
  (its OWN grain — the post-unnest grain), `keys=[passenger.id]`.
- `dim_cabins` is persisted from `select passenger.id, passenger.split_cabin`, so
  its grain is `(passenger.id, passenger.split_cabin)` — i.e. **the unnest concept
  is a grain component of the table**. Scanning it reproduces exactly the unnest
  rows → correct.

So the proposed discriminator: an UNNEST concept may be a materialized root iff a
datasource binds it AND **the concept's own address is a grain component of that
datasource** (the table is materialized at the post-unnest grain, one row per
value). The merge-onto-key unsafe case fails this test (there the grain is the key,
not the unnest concept).

## Suggested approach

In `_materialized_root_addresses`, add a narrow UNNEST branch BEFORE the
`continue` (don't loosen the existing AGGREGATE/BASIC logic):

```python
if concept.derivation == Derivation.UNNEST:
    # Safe to read from a table that materializes it AT ITS OWN GRAIN (one row
    # per unnest value — concept is a grain component), unlike a table that
    # exposes it as a coarser key (merge-onto-key), where a scan collapses rows.
    if any(
        concept.address in ds.grain.components
        and concept.address in {c.address for c in ds.output_concepts}
        and _conditions_supported(ds, where, environment.concepts)
        for ds in datasources
    ):
        out.add(concept.address)
    continue
```
(Exact shape is yours to refine — keep `_conditions_supported` gating, and match
by real `.address` not `canonical_address`, per the existing module's note about
merge-pseudonyms.)

Then confirm source-planning will actually pick `dim_cabins` once the concept is a
materialized root (it should fall into the ROOT-scan path). If it doesn't source,
trace `plan_source` / `_direct_source` for the root.

## Must-pass after the fix

1. `test_demo_e2e` passes under v4 AND v3; remove it from `tests/v4_known_failing.py`.
2. **The sibling `_PERSIST` case must stay correct**:
   `tests/persistence/test_complex_persistence.py::test_complex` (asserts `16 == 4`
   today under v4 — also a persist-of-unnest; the fix may clear it too, but DON'T
   regress its row count). It's currently tracked in `v4_known_failing.py` — check
   whether it now xpasses; if so, remove it; if it produces WRONG rows, you've hit
   the unsafe shape and the discriminator is too loose.
3. Distill a standalone parity repro into
   `local_scripts/v4_evals/cases/` (or `failing_cases/` first) — a small inline
   model: a base table with an array column, `unnest`, persist to a second table at
   the unnest grain, drop the base, select the unnest. `run_parity.py` guards it.
4. Broad regression — UNNEST is widely used, so run the unnest-heavy groups under
   v4: `TRILOGY_V4_DISCOVERY=1 pytest tests/engine tests/complex tests/persistence
   tests/modeling/hackernews` and the parity cases
   (`pytest tests/core/processing/test_v4_parity_cases.py`). Watch for the 5
   `*_unnest_*` parity cases especially. Then `ruff check . --fix; mypy trilogy;
   black .`.

## Context / gotchas

- This is v4-only; `_materialized_root_addresses` is in the v4 path, so v3 is
  unaffected — but the persist/unnest models are shared, so still sanity-check v3.
- Memory `project_v4_default_sweep_harness` / `project_v4_finer_filter_rollup`
  cover the materialized-root machinery this builds on.
- Pre-existing v4 failures NOT to worry about (fail at baseline, unrelated):
  `test_composite_rollup_aggregate_keeps_group_by`,
  `test_rowset_query_scoped_join_conflicting_filter`,
  `test_aliased_aggregate_referenced_in_having_and_order_by`.
- The broader merge work this descends from is written up in
  `local_scripts/v4_merge_fanout_brief.md`.
