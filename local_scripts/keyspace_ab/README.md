# Keyspace A/B tooling

What the keyspace phase 3 and phase 4 moves were verified with
(`docs/keyspace_phase_plan.md`). Pytest plugins and two differs. Load a plugin
with `PYTHONPATH=local_scripts/keyspace_ab` and `-p <module>`; each is inert
without its environment variable.

| file | what it does | switch |
|---|---|---|
| `ks_sqldump.py` | appends every compiled statement's SQL, per test, as JSONL | `KS_SQLDUMP=<file>` |
| `ks_rowdump.py` | appends the sorted rows of every `Executor.execute_text` result | `KS_ROWDUMP=<file>` |
| `ks_nodomains.py` | plans as before phase 4: `domains` (no region domain buckets), `basic_keys` (a BASIC keyed on its declared keys), `1` for both | `KS_NODOMAINS=<list>` |
| `ks_plans.py` | prints each plan's keyspace (outputs, in-play spans, regions) as it is built; run with `-s` | none |
| `sqlcmp.py` | `python sqlcmp.py a.jsonl b.jsonl`: tests whose DISTINCT compiled SQL differs, CTE names normalized | |
| `rowcmp.py` | `python rowcmp.py a.jsonl b.jsonl`: tests whose result sets differ, with the rows each side lacks | |
| `sqlshow.py` | `python sqlshow.py a.jsonl b.jsonl <test substring>`: the unified diff of each matching test's differing SQL | |

The phase 3 method: compute the old and the new answer side by side under
`TRILOGY_KEYSPACE_AUDIT=<file>`, plan on the old one, triage every logged
difference, flip with the audit still comparing, then strip it. The audit
module (`v4_helper/keyspace_audit.py`) was deleted once phase 5 landed; git
history has it.

The phase 4 method, since it is not byte-identical: capture rows and SQL over
the planner suites with the change switched off, then on, and triage every
test that differs in either. Cross-reference: a test whose rows moved but whose
SQL did not is a `LIMIT` without `ORDER BY`, or `now()`.

```bash
KS_NODOMAINS=1 KS_SQLDUMP=off.jsonl KS_ROWDUMP=rows_off.jsonl PYTHONPATH=local_scripts/keyspace_ab \
  .venv/Scripts/python.exe -m pytest <suites> -q -p no:cacheprovider -p ks_nodomains -p ks_sqldump -p ks_rowdump
KS_SQLDUMP=on.jsonl KS_ROWDUMP=rows_on.jsonl PYTHONPATH=local_scripts/keyspace_ab \
  .venv/Scripts/python.exe -m pytest <suites> -q -p no:cacheprovider -p ks_sqldump -p ks_rowdump
.venv/Scripts/python.exe local_scripts/keyspace_ab/sqlcmp.py off.jsonl on.jsonl
.venv/Scripts/python.exe local_scripts/keyspace_ab/rowcmp.py rows_off.jsonl rows_on.jsonl
```

Traps:

- Compare DISTINCT SQL per test, never by statement index: the benchmark tests
  compile a varying number of times per run.
- CTE names come from one pool per session, so one extra CTE early on renames
  every later statement; `sqlcmp.py` numbers them by first appearance.
- `tests/modeling/gcat/test_gcat.py::test_environment` reorders SELECT columns
  between two identical runs. It is noise, not a finding.
- Three tests fail under `ks_rowdump` only (`test_demo_aggregates`,
  `test_existence`, `test_bare_aggregate_function_concepts_resolve_at_build`):
  they index the cursor the plugin wraps.
- A plan can depend on what ran before it in the session: a `persist` into the
  shared environment adds a datasource every later statement sees. Reproduce
  an order-dependent diff by running the preceding files, not the test alone.
- A failing modeling run rewrites `zquery<N>.log`; a passing run restores them.
- A/B from two worktrees: absolute paths in file-source SQL differ by the
  worktree name. Normalize first (`sed 's/pytrilogy-base/pytrilogy-new/g'`); the
  pytest temp dirs and `trilogy_py_*` temp-table hashes still differ run to run.
- One pytest at a time. The planner suites (`tests/core tests/discovery
  tests/engine tests/join_matrix tests/modeling tests/optimization
  tests/complex`) are about 16 minutes; leave the full suite to CI.
