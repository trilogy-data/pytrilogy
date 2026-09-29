# SQL A/B

Two oracles for "did this planner change move any SQL", and one diff for both.
A green suite says nothing raised and the rows asserted still match; these say
which plans changed, including the ones no test asserts a shape for.

| oracle | covers | cost |
|---|---|---|
| `corpus_sql.py` | every SELECT of the `.preql` files you glob (202 statements over `tests/modeling`, `tests/engine`, `tests/complex`, `tests/discovery`) | 20s |
| `sqlcap.py` | every statement any test compiles, per test id (about 8,500) | one suite run, 20 min |

The base is a detached worktree, never a stash or a checkout in the shared
tree. The main tree's venv python runs it: the Rust extension is in
site-packages, and cwd puts the worktree's `trilogy` first.

```bash
git worktree add --detach ../pytrilogy-ab <base sha>
G='tests/modeling/**/*.preql tests/engine/**/*.preql tests/complex/**/*.preql tests/discovery/**/*.preql'

# corpus: once in each tree
cd ../pytrilogy-ab && ../pytrilogy/.venv/Scripts/python.exe local_scripts/sql_ab/corpus_sql.py /tmp/base.json $G
cd ../pytrilogy    && .venv/Scripts/python.exe local_scripts/sql_ab/corpus_sql.py /tmp/new.json $G
.venv/Scripts/python.exe local_scripts/sql_ab/sqldiff.py /tmp/base.json /tmp/new.json --show 40

# suite: once at each commit, both in the worktree so the main tree stays editable
cd ../pytrilogy-ab
SQLCAP_OUT=/tmp/base.jsonl PYTHONPATH=../pytrilogy/local_scripts/sql_ab \
  ../pytrilogy/.venv/Scripts/python.exe -m pytest -p sqlcap tests -q -p no:cacheprovider \
  -m "not adventureworks_execution and not clickhouse_server and not bigquery_execution and not cloud_live"
git checkout --detach <branch sha>   # and run again with SQLCAP_OUT=/tmp/new.jsonl
```

If the base predates `local_scripts/sql_ab`, run the scripts from the main
tree's copy by path; they import `trilogy` from cwd.

## Reading the diff

`MOVED <key>: n gone, m new` is a test or file whose set of statements
changed. Each pair is summarized as CTE count, JOIN count and length, and
marked `LARGER` when any of the three grew. `OUTCOME` is a test that passed on
one side and failed on the other.

Some tests move between two runs of the same code. Known ones:

- `tests/modeling/gcat/test_gcat.py::test_environment` and
  `tests/scripts/test_config.py::test_config_bootstrap*`: column order of a
  validation query.
- `tests/scripts`: `test_cli_consistency`, `test_execution_report`,
  `test_ingest`, `test_validate_agent`, and `test_trilogy`'s parallel and
  dry-run tests, which compile a varying number of statements.
- `tests/test_mocking.py::test_mock_composite_grain_fills_combination_space`.

Run a moved test twice on one tree before reading it as a plan change.

A moved plan still needs its rows checked. The modeling suites compare rows
against reference SQL; a plan that moved only in a test asserting nothing
about rows needs a look.
