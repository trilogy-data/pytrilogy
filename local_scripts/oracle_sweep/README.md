# Oracle sweep

Runs the random selects of `tests/discovery/test_oracle_sweep_laws.py`
(`--family first`) and `test_oracle_spellings_laws.py` (`--family spellings`)
past the seeds the suites commit, checks each against the raw-row oracle, and
records what the plan cost. It replaces the scratchpad scripts each
formalization round rewrote (`effsweep.py` and its kin).

```bash
# A/B the working tree against main: correctness, summary reads, scans, JOINs
python -m local_scripts.oracle_sweep ab --seeds 0:90000
python -m local_scripts.oracle_sweep ab --family first --seeds 0:20000

# one side only, to a file; `diff` compares two files
python -m local_scripts.oracle_sweep run --seeds 60000:90000 --out new.jsonl
python -m local_scripts.oracle_sweep run --ref 71b9508 --seeds 60000:90000 --out old.jsonl
python -m local_scripts.oracle_sweep diff old.jsonl new.jsonl --family spellings

# one seed: the query, the oracle's rows, and each model's SQL and rows
python -m local_scripts.oracle_sweep show 38477 [--ref origin/main]
```

`--seeds` takes ranges and lists (`5,17,100:200`) or `@file` of seeds, so a
round can replay only the seeds a diff named. `ab` and `run` exit 1 when a
query is newly wrong, raising or invalid.

## What it costs

Spellings seeds 0-90,000 x 2 models (180,000 queries), 4 cores:

| | before | now |
|---|---|---|
| one side | 1.7 h at seed-0 speed, slowing ~5x by seed 20,000 | 23 min |
| an A/B | both sides, every time | the base once per commit, then one side |

Where the time went, and what each fix is:

- **One process.** Planning is ~25 ms of CPU a query and nothing else was
  running. The sweep fans 250-seed chunks over every core (`--jobs`).
- **The 5x slowdown was CTE naming, not concepts.** An environment keeps every
  CTE name it has handed out, and `generate_cte_name` rebuilt the list of
  free names against all of them on each call: 7% of planning at seed 0 and
  most of it by seed 20,000. It now takes the first free name. The sweep also
  plans each query on a copy of the parsed model (1 ms), which gives the SQL a
  fresh executor would, byte for byte, so a record no longer depends on what
  ran before it or on `--jobs`.
- **Both sides of an A/B were run every time.** A `--ref`'s package is
  extracted once (`git archive`, as recipe step 4 does by hand) and its
  records are cached per commit under `.cache/oracle_sweep/`, keyed on the
  generators too. Main is paid for once; a wider range only runs the seeds it
  is missing.
- **11% of the spellings seeds repeat a query**; each is planned once.
- **Unchanged SQL is not run again.** In `ab`, a query whose SQL hashes to the
  base's, where the base matched the oracle, is right without executing it
  (DuckDB is 15% of a query).

## Reading the output

One record per seed and model: `status` (`ok`, `wrong`, `raise`, `invalid` for
SQL the engine rejects, `rejected` for the one error the families accept), the
SQL's hash, and counts of `tree_summary` reads, `trees` scans, JOINs and CTEs.

`ab` and `diff` print both sides' totals and then, over the queries both
planned, the seeds that moved: `broken`, `fixed`, `still_bad`, `sql_changed`,
`summary_gained`, `summary_lost`, `more_joins`, `fewer_joins`, `more_scans`.
`summary_lost_more_scans` is the subset of lost reads that also scan the trees
more: a read lost beside the same scans is not a cost (lesson 43).

## State between statements

A select defines its outputs in the environment, and some bugs need an earlier
statement's definition to show (lesson 41). The default plans every seed on a
clean model and cannot see those. `--carry-state` keeps one environment per
250-seed chunk, as the suites' shared executor does; it skips the repeat-query
shortcut, and its records are cached apart from the clean ones.
