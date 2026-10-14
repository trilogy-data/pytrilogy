# CI runtime: where the time goes and what was done about it

Profile of the `Lint and Test` workflow (`pythonpackage.yml`) on main at
`e64e74c4b`, run 38498644724, and the changes made on branch `ci-test-speed`.
The job's work is unchanged: same tests, same coverage, same matrix, no extra
parallelism. Every number here is from the GitHub Actions log timestamps or
from `--durations`, which the workflow now prints for every pytest step.

## Baseline

Wall time per matrix cell, and the pytest step inside it:

| cell           | job     | pytest step |
|----------------|---------|-------------|
| ubuntu 3.11    | 24.1 m  | 1158 s      |
| ubuntu 3.12    | 21.2 m  |  939 s      |
| ubuntu 3.13    | 23.4 m  | 1006 s      |
| windows 3.11   | 42.4 m  | 1827 s      |
| windows 3.12   | 26.7 m  | 1064 s      |
| windows 3.13   | 26.5 m  | 1007 s      |

The pytest step is 70-75% of every cell. The rest is the Rust build and tests
(60-180 s), dependency install (30-130 s, Windows slowest), the uv script
warm-up (25-90 s), lint (30-80 s), the rich re-run (80-100 s) and the packaged
CLI smoke test (30-70 s).

Pytest step on ubuntu 3.13 (1006 s), attributed per test directory from the
progress-line timestamps:

| directory         | seconds | note |
|-------------------|---------|------|
| tests/scripts     | 537     | CLI tests through `CliRunner`, in-process |
| tests/modeling    | 127     | TPC-DS / TPC-H / gcat engines |
| tests/engine      | 116     | includes live BigQuery and Snowflake queries |
| tests/*.py        |  51     | |
| tests/complex     |  33     | 31 s of it is one file (see Lark below) |
| collection        |  17     | 11k items (40 s on Windows) |
| session teardown  |  14     | between the last test and the summary |
| everything else   | ~110    | |

Running `tests/scripts` alone, locally, takes 352 s for 1521 tests; the same
tests inside the full session take 537 s on a Linux runner and 726 s on
Windows. The difference is the mechanism below, not the tests.

## Result

Same workflow, run 38597060869 on this branch (the GC gate, the Lark grammar,
the serve test and the uv install; the parser cache step came later):

| cell           | job before | job after | pytest before | pytest after |
|----------------|-----------:|----------:|--------------:|-------------:|
| ubuntu 3.11    | 24.1 m     | 24.8 m    | 1158 s        | 1154 s       |
| ubuntu 3.12    | 21.2 m     | 17.1 m    |  939 s        |  709 s       |
| ubuntu 3.13    | 23.4 m     | 12.6 m    | 1006 s        |  536 s       |
| windows 3.11   | 42.4 m     | 24.5 m    | 1827 s        | 1056 s       |
| windows 3.12   | 26.7 m     | 25.7 m    | 1064 s        |  945 s       |
| windows 3.13   | 26.5 m     | 24.3 m    | 1007 s        |  893 s       |

Dependency install went from 30-130 s to 5-26 s on every cell, and the rich
re-run from 80-100 s to 32-66 s. Runner speed varies a lot between runs (the
same lint steps took 15 s on one ubuntu cell and 35 s on another in the same
run), so single cells are indicative; the ubuntu 3.13 and windows 3.11 cells
ran on healthy runners both times and show the step-level effect.

ubuntu 3.11 did not move. Its slowest tests are the same CPU-bound ones as on
3.13 at 4-10x the cost: `test_ingest_output_passes_integration` 75 s against
8 s, the first Lark parse 31 s against 6 s, the Lark/pest corpus parity
13 s against 3 s. That cell runs coverage with the C tracer (`sysmon` needs
3.12+; see `[tool.coverage.run]` in pyproject.toml), which the project
measured at ~150% overhead on this suite against ~60% for sysmon, on an
interpreter that is itself slower. See the targets below.

## Findings and fixes

### 1. A full garbage collection per DuckDB executor close

`Executor.close()` called `gc.collect()` for every DuckDB executor, and a CLI
invocation closes at least one. A full collection walks the whole live heap, so
its cost is set by how much state the session has accumulated, not by the test:

| `tests/scripts/test_config.py` (81 tests) | seconds |
|---|---|
| alone                                             | 3.2  |
| after `tests/engine` + `tests/discovery` ran first | 20.7 |
| same, with the collection gated (this branch)     | 1.2  |

In the cProfile of four CLI test files `gc.collect` was 25% of the wall time
with a small heap; after a few thousand tests it dominates. The collection
exists for a long-lived process cycling engines, so that DuckDB instances held
by reference cycles are freed promptly; it is not something a test session
needs, where the interpreter's own collections reclaim the same cycles. It is
now gated on `CONFIG.collect_on_close` (default on) and turned off in
`tests/conftest.py`, the same change as the open PR #718. Locally,
`tests/engine tests/discovery tests/scripts/test_config.py` went from 311 s
to 197 s with the same pass/fail set, and `tests/scripts` on its own from
352 s to 118 s, 1521 passed both times.

Expected CI effect: most of the 185 s gap between `tests/scripts` alone and
in-session, on every cell; larger on Windows.

### 2. Lark grammar expansion: 100k rules, 250 s build, 51 MB cache

The `datasource` rule had fourteen `?` clauses and `select_statement` seven.
Lark's LALR front end expands `a? b? c?` into one alternative per subset, so
`datasource` alone became ~50k alternatives. Consequences:

- a cold grammar build of 255 s locally; 31 s on a Linux runner and 138 s on
  Windows, paid once per job by the first test that touches the Lark backend
  (`tests/complex/test_by_on_non_aggregate_error.py`, 20 trivial parse tests,
  showed as 31 s / 138 s);
- a 51 MB parser cache in the temp dir that took 4.4 s to unpickle in every
  process that used the Lark backend (the `--validate` fallback, subprocess
  tests, the CLI when the pest wheel is absent);
- 1.5 GB of RSS during the build.

Trailing clauses are now inlined nullable rules (`_x_opt: x |`), which add one
rule each and produce the same tree. The modifiers before a statement keyword
stay `?`: a nullable rule there has to reduce on the keyword, and Lark resolves
the resulting shift/reduce conflict in favour of any other statement that
shifts the same keyword (`natural_select_statement` for `select`).

| | before | after |
|---|---|---|
| expanded rules | 100,145 | 1,373 |
| cold build     | 255 s   | 5.7 s |
| cache size     | 51 MB   | 1.4 MB |
| cache load     | 4.4 s   | 0.1 s |

Verified by parsing every `.preql` file in the tree (13,465 files, including
eval workspaces) with both grammars: 13,332 identical trees, 133 identical
failures, 0 differences in outcome. The only textual difference is the
"Expected one of" token list inside raw Lark error messages, which the custom
error path does not surface. The 991 parser tests in `tests/complex`,
`tests/parsing`, `tests/rendering` and `test_parse_engine_v2.py` pass.

### 3. `test_serve_cli` idled out its server

The test started `serve --timeout 12`, hit two endpoints in under a second,
then waited for the timeout to end the server: 12.7 s per run. Its sibling
test already uses the `stop_servers` fixture; this one now does too.

### 4. Dependency installation

`pip install -r requirements.txt -r requirements-test.txt` took 30-50 s on
Linux and 95-130 s on Windows with a warm cache. uv is already installed in
the job for the script tests; it now installs the dependencies too, against the
cache `setup-uv` restores. The separate
`pip install sqlalchemy-bigquery --ignore-requires-python` line was dropped:
the package is in `requirements-test.txt` and has required only `>=3.10` for
some time. The per-step `pip install pytest pytest-cov` lines were redundant
with `requirements-test.txt`.

### 5. The Lark parser tables are cached across runs

Even at 6 s (31 s on 3.11), the LALR build is paid by every job for an input
that changes only when the grammar or the lark version does. Lark writes the
tables next to a hash of grammar, options, lark version and Python version in
the interpreter's temp dir, and verifies that hash on load, so a wrong restore
is just a miss. A `actions/cache` step keyed on the same inputs keeps the file
between runs.

### 6. `--durations` on every pytest step

Per-file timing from progress lines cannot see a slow parametrization or a
session-fixture teardown. Every pytest step now reports its slowest tests, so
the next profile starts from the log.

## Targets not taken (yet), in order of expected value

- **Coverage core on 3.11 (needs a decision).** The two 3.11 cells are now
  the longest in the matrix (24-25 min), and most of the difference from 3.13
  is the C tracer. The options are to accept it, or to measure coverage on the
  3.12/3.13 cells only and run 3.11 without `--cov`; the Codecov report would
  lose the 3.11 flag, whose only unique lines are version-specific branches.
  Not done here because the brief was no loss of coverage.
- **The 13 s gap before the last test file, 23 s on Windows.** Between the
  progress line for `tests/test_window_where_pushdown_matrix.py` and the one
  for `tests/vscode/test_vscode_test_cases.py`, with no setup, call or
  teardown phase over 2.3 s in the `--durations` list. Whatever it is runs
  outside every reported phase, which points at the interpreter (a generation
  2 collection on a very large heap would look like this) rather than a
  fixture. Not reproduced locally.
- **sf=1 TPC-DS generation, 17-25 s per job.** `dsdgen(sf=1)` plus the
  parquet export runs on every job because the files are gitignored. The set
  is 385 MB; caching it is possible but would be the largest entry in the
  repository's 10 GB cache budget for a ~10 s gain.
- **Live-service tests.** `tests/engine/test_bigquery.py` (11 s), the
  Snowflake tests, `tests/ai/test_providers_basic.py` (13 s, 3.13 cell only),
  `tests/scripts/test_readme_quickstart.py` (13 s, fetches public models) and
  `tests/scripts/test_trilogy.py::test_bigquery` run real network calls. They
  are the coverage they provide; the only lever is batching, not removal.
- **`test_cli_consistency.py` (40 s on CI).** Ingests the sf=0.01 TPC-DS set
  twice and runs `integration` on it. `integration` issues one query per
  declared column domain (`validate_declared_domains`): 3,779 generated queries
  for 24 datasources, most of the 12 s locally. Batching the per-column domain
  checks into one query per datasource keeps the validation and would cut most
  of it, but changes the validation module's result shape and belongs in its
  own PR.
- **`test_derived_key_domain_seeds.py` (21 s / 50 s Windows).** Four
  subprocesses under coverage, each importing trilogy and the test helpers.
  The hash-seed sweep needs separate interpreters; the per-process cost is
  import time, which the Lark fix already lowers when the subprocess reaches
  the Lark backend.
- **Collection, 17 s / 40 s.** 11k items; nothing dominates (pandas, plotext,
  altair and the BigQuery client each add ~0.5 s at import). The `-k` re-run
  after installing rich re-collects everything to select ~390 tests; pointing
  it at the directories that contain them would save the collection pass.
- **Rust test step, 36-106 s.** `cargo test --release` with `lto = true`
  links every test binary through LTO. A debug-profile `cargo test` compiles
  far faster and keeps overflow and debug assertions on; the pest parser runs
  slower in debug, so this needs measuring on a runner.
- **Packaged-CLI smoke test, 30-70 s.** A second venv and a second maturin
  build of the extension. Installing the wheel the earlier `pip install -e .`
  could have produced would make it a packaging check without the rebuild.
