# Keyspace A/B tooling

What the keyspace phase 3 moves were verified with (`docs/keyspace_phase_plan.md`).
Three pytest plugins and one differ. Load a plugin with
`PYTHONPATH=local_scripts/keyspace_ab` and `-p <module>`; each is inert without
its environment variable.

| file | what it does | switch |
|---|---|---|
| `ks_sqldump.py` | appends every compiled statement's SQL, per test, as JSONL | `KS_SQLDUMP=<file>` |
| `ks_rowdump.py` | appends the sorted rows of every `Executor.execute_text` result | `KS_ROWDUMP=<file>` |
| `ks_plans.py` | prints each plan's keyspace (outputs, in-play spans, regions) as it is built; run with `-s` | none |
| `sqlcmp.py` | `python sqlcmp.py a.jsonl b.jsonl`: tests whose DISTINCT compiled SQL differs | |

The method: compute the old and the new answer side by side under
`TRILOGY_KEYSPACE_AUDIT=<file>`, plan on the old one, triage every logged
difference, flip with the audit still comparing, then strip it. Confirm with a
SQL dump before and after:

```bash
KS_SQLDUMP=off.jsonl PYTHONPATH=local_scripts/keyspace_ab \
  .venv/Scripts/python.exe -m pytest <files> -q -p no:cacheprovider -p ks_sqldump
# ... apply the change, dump to on.jsonl ...
.venv/Scripts/python.exe local_scripts/keyspace_ab/sqlcmp.py off.jsonl on.jsonl
```

Traps:

- Compare DISTINCT SQL per test, never by statement index: the benchmark tests
  compile a varying number of times per run.
- `tests/modeling/gcat/test_gcat.py::test_environment` reorders SELECT columns
  between two identical runs. It is noise, not a finding.
- A failing modeling run rewrites `zquery<N>.log`; a passing run restores them.
- One pytest at a time. The planner suites (`tests/core tests/discovery
  tests/engine tests/join_matrix tests/modeling tests/optimization
  tests/complex`) are about 20 minutes; leave the full suite to CI.
