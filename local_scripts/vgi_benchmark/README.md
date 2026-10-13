# VGI transport benchmark

`bench.py` compares the DuckDB python-datasource transports end to end through
the trilogy executor: `shell` (the `uv_run` macro) and `vgi` (the
`trilogy.dialect.duckdb_vgi` bridge worker). Every source is a generated
`uv run` script that streams N rows of `(id int64, value float64, label string)`.

`resident_worker.py` / `resident_bench.py` prototype the next step: the source
*is* the VGI worker, which DuckDB pools, so no process starts per query. These
run against haybarn directly, not through trilogy.

```
pip install "pytrilogy[vgi]"   # haybarn + vgi-python, python >= 3.13
python local_scripts/vgi_benchmark/bench.py --rows 1000 1000000 10000000 --reps 7 --output out.json
python local_scripts/vgi_benchmark/resident_bench.py local_scripts/vgi_benchmark/resident_worker.py 7 1000 1000000 10000000
```

## Results (Windows 11, 16 cores, haybarn 1.5.5rc1, vgi-python 0.43.1)

Medians of 7 warm queries. `full_scan` reads all 3 columns; `one_column` reads
`sum(value)`.

| rows | query | shell | vgi bridge | resident |
| --- | --- | --- | --- | --- |
| 1k | full_scan | 0.350s | 0.183s | 0.003s |
| 1k | one_column | 0.408s | 0.274s | 0.003s |
| 1M | full_scan | 0.481s | 0.185s | 0.036s |
| 1M | one_column | 0.495s | 0.171s | 0.010s |
| 10M | full_scan | 1.965s | 0.641s | 0.242s |
| 10M | one_column | 2.419s | 0.346s | 0.063s |

On Windows, `shell` stages 28.9MB (1M rows) / 288.9MB (10M rows) of Arrow to
disk per scan; `vgi` stages nothing.

Caveats:

- **`uv run` startup is the noise floor.** It measured 0.55s to 4s on this
  machine (a 3-4s outlier now and then), and both `shell` and the bridge pay it
  on every scan. Compare medians and rerun before drawing small-row
  conclusions.
- **Setup cost is higher for `vgi`.** The first `INSTALL vgi` download plus
  spawning the worker took ~1.1-5s, against 0.7-2.5s for `shell`. Resident
  setup was 12s the first time, because uv resolved `vgi-python` into the
  script's environment; that is cached after.
- **POSIX was not measured.** There the shell transport is a `shellfs` pipe
  with no disk staging, so its gap to the bridge should be smaller than on
  Windows.
