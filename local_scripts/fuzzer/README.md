# Deterministic Trilogy fuzzer

This is a bounded differential fuzzer: it generates small, readable Trilogy
programs and compares them with independent DuckDB SQL oracles over the same
query-backed rows. It is deterministic, has no network or LLM dependency, and
does not generate arbitrary syntax.

The fixed datasets deliberately include:

- nullable and required attributes, `is null`, `is not null`, and boolean `not`;
- duplicate and null join keys plus side-exclusive key domains;
- different many-to-many fanout ratios across two facts;
- zero, negative, and repeated measures.

The generated families cover scalar filters, grouped aggregates, WHERE/HAVING,
nested and membership rowsets, filtered/statistical functions, ROLLUP/CUBE,
aggregate and partitioned windows, relational `union(...)`, derived
`subset join`/`union join`, multiway and composite domain joins, offset join
expressions, nested parent-grain aggregates, chasm/fanout aggregation,
subordinate keys crossing rowset boundaries, and named grouping-derived
window partitions under ROLLUP/CUBE.

Run everything from the repository root:

```powershell
.venv/Scripts/python.exe -m local_scripts.fuzzer
```

Useful slices:

```powershell
# Inspect the corpus without running it
.venv/Scripts/python.exe -m local_scripts.fuzzer --list

# Deterministic ten-case sample
.venv/Scripts/python.exe -m local_scripts.fuzzer --seed 17 --limit 10

# Add 20 reproducible randomized datasets, each with roughly 40 event rows
.venv/Scripts/python.exe -m local_scripts.fuzzer `
  --random-only --random-datasets 20 --data-seed 5000 --random-rows 40

# Specific coverage
.venv/Scripts/python.exe -m local_scripts.fuzzer --family join --dataset edge
.venv/Scripts/python.exe -m local_scripts.fuzzer --tag rowset --tag nullable
.venv/Scripts/python.exe -m local_scripts.fuzzer --case rank_in_having
```

Random dataset `N` is named `random_00NNNN`. Its rows depend only on
`--data-seed`, its offset, and `--random-rows`; rerunning those arguments
recreates the same programs exactly. `--seed` only controls case ordering and
sampling by `--limit`.

Every run writes `summary.json` and `metrics.csv` beneath `runs/`. Per-case
metrics include oracle, compilation, and execution time plus generated SQL
character and byte counts; the JSON also rolls timing and SQL size up by family.

Any failure gets a stable directory beneath `repros/<case-id>/` containing:

- `repro.preql`: complete query-backed data model and Trilogy query;
- `oracle.sql`: standalone expected-result query;
- `generated.sql`: Trilogy's SQL, when compilation succeeded;
- `result.json`: rows, error stage, timings, and sizes;
- `README.md`: concise handoff instructions.

Both output directories are ignored by Git. Copy a repro elsewhere when it
should become a committed regression test or bug handoff.
