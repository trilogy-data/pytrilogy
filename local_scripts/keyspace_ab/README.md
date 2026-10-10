# Row A/B tooling

A pytest plugin and a differ for "did this planner change move any rows". For
SQL, use `local_scripts/sql_ab/` (`sqlcap.py` + `sqldiff.py`).

| file | what it does | switch |
|---|---|---|
| `ks_rowdump.py` | appends the sorted rows of every `Executor.execute_text` result, per test, as JSONL | `KS_ROWDUMP=<file>` |
| `rowcmp.py` | `python rowcmp.py a.jsonl b.jsonl`: tests whose result sets differ, with the rows each side lacks | |

Load the plugin with `PYTHONPATH=local_scripts/keyspace_ab` and `-p ks_rowdump`;
it is inert without its environment variable. Capture once at each commit (the
base from a detached worktree, as in the SQL A/B README), then compare:

```bash
KS_ROWDUMP=rows_base.jsonl PYTHONPATH=local_scripts/keyspace_ab \
  .venv/Scripts/python.exe -m pytest <suites> -q -p no:cacheprovider -p ks_rowdump
.venv/Scripts/python.exe local_scripts/keyspace_ab/rowcmp.py rows_base.jsonl rows_new.jsonl
```

Traps:

- A test whose rows moved but whose SQL did not is a `LIMIT` without `ORDER BY`,
  or `now()`.
- Three tests fail under `ks_rowdump` only (`test_demo_aggregates`,
  `test_existence`, `test_bare_aggregate_function_concepts_resolve_at_build`):
  they index the cursor the plugin wraps.
- A plan can depend on what ran before it in the session: a `persist` into the
  shared environment adds a datasource every later statement sees. Reproduce
  an order-dependent diff by running the preceding files, not the test alone.
- One pytest at a time. The planner suites (`tests/core tests/discovery
  tests/dialect tests/engine tests/join_matrix tests/modeling
  tests/optimization tests/complex`) are about 16 minutes; leave the full
  suite to CI.
