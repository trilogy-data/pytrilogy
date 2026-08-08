# Handoff — 3 failing tests on `v4_more_parity_work_three`

Written s55 (2026-07-31). Owner: unassigned. **Not investigated** — this is a
triage list with reproductions and signatures, nothing more. Each item needs its
own diagnosis.

## Status of the branch

Serial full suite (`pytest tests -m "not adventureworks_execution"`):

```
3 failed, 6266 passed, 102 skipped, 6 xfailed, 11 xpassed in 1565.70s
```

The skipped / xfailed / xpassed counts match the s53 baseline exactly. These 3
are the only failures.

**They are recent.** `local_scripts/handoff_failing_tests.md` closed out an
earlier batch at **6259 passed, 0 failed** on this same branch, so all three
appeared after that — i.e. during the work that followed it, not from any
long-standing defect. (6259 -> 6266 passed is accounted for: s55 added 7 tests to
`tests/core/processing/test_v4_network_search.py`.) Checking what landed between
that handoff and now is probably the fastest route to all three.

## These are PRE-EXISTING — do not blame the s55 network_search work

s55 reworked `trilogy/core/processing/v4_helper/network_search.py` (dedup +
`ObligationKind` + reverse-BFS `chain_completers`). All three failures reproduce
identically with **HEAD's** version of that module, so they predate it.

How that was established, in case it needs redoing for another change — an
in-process A/B that touches **no file in the git tree** (never `git stash` /
`checkout` here, see `feedback_never_git_stash_parallel_agents`):

```python
# conftest-style plugin: let trilogy import normally, then exec HEAD's source
# into a fresh module and rebind the entry points source_planning holds.
git show HEAD:trilogy/core/processing/v4_helper/network_search.py > /scratch/ns_head.py

def pytest_configure(config):
    import trilogy.core.processing.v4_helper.source_planning as sp
    spec = importlib.util.spec_from_file_location("_ns_head", OLD)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    sp.build_source_network = module.build_source_network
    sp.search_sources = module.search_sources
    sp.SourceNetwork = module.SourceNetwork
```

Results, both legs, whole files: `test_duckdb.py` → `2 failed, 136 passed,
1 skipped` identical; `test_gcat.py` → `1 failed, 35 passed` identical. A
**control leg** that neuters `search_sources` shifts the results (3 failed,
different set), proving the rebind point is load-bearing and the A/B has teeth.

**Run tests one process at a time.** Concurrent pytest runs share the DuckDB
files under `tests/modeling/*/memory` and manufacture failures — an overlapping
run this session reported a 4th failure at ~37% that did not reproduce serially.
See `feedback_never_run_pytest_suites_concurrently`.

---

## 1 + 2. `tests/engine/test_duckdb.py` — the q94 nested-filter shape

```
tests/engine/test_duckdb.py::test_nested_filter_per_key_aggregate_membership_not_in_group_by
tests/engine/test_duckdb.py::test_nested_filter_per_key_aggregate_membership_executes
```

Almost certainly ONE bug with two tests on it. Both use
`_NESTED_FILTER_AGG_MEMBERSHIP_MODEL` / `_NESTED_FILTER_AGG_MEMBERSHIP_QUERY`
(`tests/engine/test_duckdb.py:1006-1051`).

The query — a filtered-membership key whose condition mixes a **nested filter
concept** with **per-key aggregates**:

```
auto applicable_orders <- order_number ? state = 'IL';
auto qualifying_orders <- order_number
  ? applicable_orders is not null
    and count_distinct(warehouse) by order_number > 1
    and bool_or(is_returned) by order_number is not true;
where order_number in qualifying_orders
select order_number as o, sum(profit) as total_profit
order by o;
```

**Both now fail the same way, and it is NOT the failure the tests were written
for.** Both die in rendering:

```
ValueError: Could not render the query: Missing source reference to local.warehouse.
A planned reference has no backing source CTE -- typically an unsupported cross-rowset
or membership shape the planner could not wire.
```

Note what that means for the assertions: `..._not_in_group_by` was written to
assert no aggregate appears in a `GROUP BY`, and `..._executes` to assert
`[(100, 30.0)]`. Neither assertion is reached — `generate_sql` raises first. So
the ORIGINAL defect (aggregates rendered into `GROUP BY`, DuckDB "GROUP BY clause
cannot contain aggregates") may or may not still be present; it is masked.

First thing to check: the first CTE selects `item_id, order_number, profit,
state` from `sales` but **not `warehouse`**, even though the datasource binds it
and `count_distinct(warehouse) by order_number` needs it. So the projection that
feeds the per-key aggregate is dropping the aggregate's own argument.

Per `feedback_token_swings_and_binder_errors_are_framework`, a render error out
of generated SQL is a framework bug, not an authoring problem.

Repro:

```bash
.venv/Scripts/python.exe -m pytest tests/engine/test_duckdb.py \
  -k nested_filter_per_key_aggregate_membership -q --tb=long
```

---

## 3. `tests/modeling/gcat/test_gcat.py::test_case_key`

```
assert "_launch_code" in sql[0]
```

The test wants the derived `_launch_code` concept to be materialized once and
referenced by name. Instead the generated SQL **inlines its whole CASE
expression twice**, once per branch of the outer CASE:

```sql
CASE
WHEN CASE WHEN ( "launch_info"."LaunchCode" is null or "launch_info"."LaunchCode" = '' ) = True
     THEN null ELSE SUBSTRING("launch_info"."LaunchCode",1,1) END = 'O' THEN 'Orbital'
WHEN CASE WHEN ( "launch_info"."LaunchCode" is null or "launch_info"."LaunchCode" = '' ) = True
     THEN null ELSE SUBSTRING("launch_info"."LaunchCode",1,1) END = 'D' THEN 'Deep Space'
ELSE 'Other'
END as "launch_filter"
```

So this is a **CSE / materialization** question, not a wrong-rows one — the SQL
looks semantically correct, it just duplicates the subexpression instead of
projecting `_launch_code`. Worth deciding first whether the test's expectation is
still the intended contract (does the planner still promise a named projection
for a derived concept used inside another derivation?) before chasing the
planner. If the contract changed deliberately, the test is the thing to update.

Repro:

```bash
.venv/Scripts/python.exe -m pytest tests/modeling/gcat/test_gcat.py::test_case_key -q --tb=long
```

Note `tests/modeling/gcat` has ZERO `WHERE` statements corpus-wide, so a clean
run there proves little about predicate handling — see
`reference_predicate_audit_corpus_coverage`.

---

## Gate for whatever fixes these

- `pytest tests -m "not adventureworks_execution"` — expect **6266+ passed,
  102 skipped, 6 xfailed, 11 xpassed**; the skip/xfail/xpass counts are part of
  the signal. Run it ALONE.
- Corpus non-regression: render all 109 TPC-DS + 23 TPC-H queries before and
  after and diff byte-for-byte (132/132 identical is the current state). Gate
  against the CURRENT TREE, not the checked-in goldens — see
  `feedback_gate_against_tree_not_goldens`.
- `mypy trilogy`, `ruff check --select E,F,I <changed>`, `black .`
