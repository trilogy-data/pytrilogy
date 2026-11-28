# SQL A/B

Two oracles for "did this planner change move any SQL", and one diff for both.
A green suite says nothing raised and the rows asserted still match; these say
which plans changed, including the ones no test asserts a shape for.

| oracle | covers | cost |
|---|---|---|
| `corpus_sql.py` | every SELECT of the `.preql` files you glob (202 statements over `tests/modeling`, `tests/engine`, `tests/complex`, `tests/discovery`) | 20s |
| `sqlcap.py` | every statement any test compiles, per test id (about 31,800) | one suite run, 20 min; 7 min under `-n 4` |

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

Under xdist (`-n 4`) each worker writes `<SQLCAP_OUT>.gwN.jsonl` beside the
named path, and `sqldiff.py` given the named path reads them as one capture.

The corpus is a small sample: it is blind to plans only the suite compiles.
Dropping one of the five `preserve_keys` sources (join-axis audit, part 33)
moved 0 corpus statements and well over a hundred suite ones.

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
- `tests/core/validation/test_validation_laws.py::test_grain_check_exact` and
  `tests/scripts/test_ingest.py::test_ingest_output_round_trips`: column order
  of the grain check.
- `tests/windows/test_window_laws.py` and
  `tests/engine/test_rowset_declared_join_pairing.py`: GROUP BY and column
  order under xdist.
- `tests/modeling/tree_reporting/test_dashboard_queries.py::test_dashboard_served_from`:
  the number of statements a worker compiles.

Run a moved test twice on one tree before reading it as a plan change.

A moved plan still needs its rows checked. The modeling suites compare rows
against reference SQL; a plan that moved only in a test asserting nothing
about rows needs a look.

## Join census

`join_census.py` answers a different question from the two oracles above: not
"did the SQL move" but "where did each merge's join keys come from". It
compiles the globbed corpus with the planner instrumented and reports, per
statement, the FINAL merge's side count, which stage-2 and stage-3 join-key
paths fired (preserve keys, fresh ROOT re-source, widening, bridging, regraft),
the keys `get_node_joins` actually used, and whether the planner's own FINAL
join plan (`v4_helper/join_plan.py`) stated the same keys. `join_census_plugin.py`
runs the same check over every statement a suite compiles: it resolves each
planned FINAL merge twice, unrestricted and restricted, and writes one line
per merge whose unrestricted keys leave the plan's permitted set (with the
plan and each side's resolved columns). Its double resolve can trip module
fixtures, so confirm a failure without the plugin before reading it.

```bash
CENSUS_OUT=/tmp/census.jsonl PYTHONPATH=local_scripts/sql_ab   python -m pytest -p join_census_plugin tests -q -p no:cacheprovider -m "..."
```

```bash
python local_scripts/sql_ab/join_census.py /tmp/census.json "tests/modeling/**/*.preql" "tests/engine/**/*.preql" "tests/complex/**/*.preql" "tests/discovery/**/*.preql"
```

## Rank census

`rank_census_plugin.py` asks which terms of the order the join walk reads
(`join_resolution._stated_order`) decide a walk. It walks every merge of three
or more sides again once per term, with that term held constant, and counts
the merges whose join order moves: the terms of the key placement
(the key's shape: `authored`, `solid`, `held`, `max_held`, `holders`), the holders' shape places
(`holder_shape`) and the shape terms (`binding_tiers`, `plan_order`, `hosts`).
Sum the per-worker files' `counts`.

```bash
RANK_OUT=/tmp/rank PYTHONPATH=local_scripts/sql_ab python -m pytest -p rank_census_plugin tests -q -p no:cacheprovider -n 4 -m "not adventureworks_execution"
```

## Walk flip

`walk_flip_plugin.py` asks whether the shape of the join walk's order decides
rows. It reverses the shape terms (`shape`: each key's authored, solid,
held span, most-held and holder count, and each side's `~` tiers, plan
order and hosting) and the side name that breaks their ties (`name`),
through `tests/helpers/walk_order.py`. No term is left out: the order has
no row term. A test that fails only under the flip either pins a shape (read its
assertion) or has rows that depend on the shape, which is a bug.

```bash
WALK_FLIP=shape,name PYTHONPATH=local_scripts/sql_ab python -m pytest -p walk_flip_plugin tests -q -p no:cacheprovider -n 4 -m "not adventureworks_execution"
```

## Narrowing census

`upgrade_census_plugin.py` records every firing of the optimizer's
`UpgradeOuterFromKeySetEquivalence` (seam 2 of
`docs/query_gen_simplification_audit_2026_10.md`): the method (`inner`,
`narrow`, `narrow_veto` under an authored FULL key), the join types before and
after, and the proof helpers that held while it was decided. Grouping firings
by proof set names the class a plan-time proof would have to take over.

```bash
UPGRADE_CENSUS=/tmp/up.jsonl PYTHONPATH=local_scripts/sql_ab python -m pytest -p upgrade_census_plugin tests -q -p no:cacheprovider -n 4 -m "not adventureworks_execution"
```

`null_safe_census_plugin.py` does the same for `SimplifyNullSafeJoins`: every
key it strips the null-safe form from, the `proven_non_null` layer that proved
it on each side, and whether the planner already had the proof (the planned
merge's own condition, and the type it planned the join).

```bash
NULL_SAFE_CENSUS=/tmp/ns.jsonl PYTHONPATH=local_scripts/sql_ab python -m pytest -p null_safe_census_plugin tests -q -p no:cacheprovider -n 4 -m "not adventureworks_execution"
```

`twins_census_plugin.py` traces each equivalence upgrade back to the planned
merge: per key, the planner's twin proof (`_twins`) and the first test of it
that failed, with the joins that dropped a side's last key provider and the
left an earlier INNER join equated the key with (the optimizer's witness).
Group the verdicts to name the class a plan-time proof has to take over next.

```bash
TWINS_CENSUS=/tmp/tw.jsonl PYTHONPATH=local_scripts/sql_ab python -m pytest -p twins_census_plugin tests -q -p no:cacheprovider -n 4 -m "not adventureworks_execution"
```

## Region census

`domain_kind_plugin.py` records, per test, each region domain the planner
decides (`region_domains.DomainKind`, with the note of a PADDED one) and each
extent election, marking a span `domain` when a region domain owns it and
`ranked` when the election's ranking picked its owner (seam 1).

```bash
DOMAIN_CENSUS=/tmp/kinds.jsonl PYTHONPATH=local_scripts/sql_ab python -m pytest -p domain_kind_plugin tests -q -p no:cacheprovider -n 4 -m "not adventureworks_execution"
```
